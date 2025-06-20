package com.example.migrator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MigrationService {
    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

    static final Counter processedCounter = Counter.build()
            .name("migrator_processed_total")
            .help("Total processed records")
            .register();

    static final Counter errorCounter = Counter.build()
            .name("migrator_errors_total")
            .help("Total errors during migration")
            .register();

    static final Gauge speedGauge = Gauge.build()
            .name("migrator_speed")
            .help("Average records per second")
            .register();

    private final Config config;

    public MigrationService(Config config) {
        this.config = config;
    }

    public void run() throws Exception {
        log.info("Starting migration task {}", config.taskName());
        long start = System.nanoTime();
        try (Connection src = DriverManager.getConnection(config.sourceUrl(), config.sourceUser(), config.sourcePassword());
             Connection dst = DriverManager.getConnection(config.targetUrl(), config.targetUser(), config.targetPassword())) {

            log.info("Ensuring progress table");
            ensureProgressTable(dst);
            log.info("Loading last processed id for task {}", config.taskName());
            String lastId = loadProgress(dst, config.taskName());
            log.info("Last processed id: {}", lastId);
            List<String> ids = config.ids();
            if (ids == null) {
                log.info("Fetching ids from source");
                ids = fetchIds(src, lastId);
            } else if (lastId != null) {
                log.info("Resuming from id {}", lastId);
                ids = ids.subList(ids.indexOf(lastId) + 1, ids.size());
            }

            log.info("Total ids to process: {}", ids.size());
            processIds(src, dst, ids, start);
        }
        log.info("Migration finished. Total processed {}", (int)processedCounter.get());
    }

    private List<String> fetchIds(Connection src, String lastId) throws Exception {
        List<String> ids = new ArrayList<>();
        String query = "SELECT id FROM person WHERE birthDay > current_date - interval '18 years'" +
                (lastId != null ? " AND id > '" + lastId + "'" : "") +
                " ORDER BY id";
        log.info("Fetching ids with query: {}", query);
        try (Statement st = src.createStatement(); ResultSet rs = st.executeQuery(query)) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        }
        log.info("Fetched {} ids", ids.size());
        return ids;
    }

    private void processIds(Connection src, Connection dst, List<String> ids, long start) throws Exception {
        if (ids.isEmpty()) {
            return;
        }
        CopyManager srcCopy = src.unwrap(PGConnection.class).getCopyAPI();
        CopyManager dstCopy = dst.unwrap(PGConnection.class).getCopyAPI();

        int batchSize = config.batchSize();
        List<String> batch = new ArrayList<>(batchSize);
        int processed = 0;
        for (String id : ids) {
            batch.add(id);
            if (batch.size() >= batchSize) {
                copyBatch(srcCopy, dstCopy, dst, batch);
                processedCounter.inc(batch.size());
                processed += batch.size();
                double duration = (System.nanoTime() - start) / 1_000_000_000.0;
                double speed = processed / duration;
                speedGauge.set(speed);
                log.info("Processed {} of {} ids ({} recs/sec)", processed, ids.size(), String.format("%.2f", speed));
                saveProgress(dst, config.taskName(), batch.get(batch.size() - 1));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            copyBatch(srcCopy, dstCopy, dst, batch);
            processedCounter.inc(batch.size());
            processed += batch.size();
            double duration = (System.nanoTime() - start) / 1_000_000_000.0;
            double speed = processed / duration;
            speedGauge.set(speed);
            log.info("Processed {} of {} ids ({} recs/sec)", processed, ids.size(), String.format("%.2f", speed));
            saveProgress(dst, config.taskName(), batch.get(batch.size() - 1));
        }
    }

    private void copyBatch(CopyManager srcCopy, CopyManager dstCopy, Connection dst, List<String> batch) throws Exception {
        String inList = batch.stream().map(id -> "'" + id + "'").reduce((a,b) -> a + "," + b).orElse("'0'");
        String copyOutSql = "COPY (SELECT id, birthday FROM person WHERE id IN (" + inList + ")) TO STDOUT WITH (FORMAT CSV)";
        String copyInSql = "COPY kids (id, birthday) FROM STDIN WITH (FORMAT CSV)";
        try (var srcWriter = new StringWriter();
             var writer = new StringWriter();
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            log.debug("Copying batch of {} ids", batch.size());
            // Copy data from the source database into a string
            srcCopy.copyOut(copyOutSql, srcWriter);
            // Parse each line and reprint using CSVPrinter to normalise CSV format
            new BufferedReader(new StringReader(srcWriter.toString())).lines().forEach(line -> {
                try {
                    printer.printRecord(line.split(",", 2));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            printer.flush();
            byte[] data = writer.toString().getBytes();
            dst.setAutoCommit(false);
            try {
                dstCopy.copyIn(copyInSql, new ByteArrayInputStream(data));
                dst.commit();
            } catch (Exception e) {
                errorCounter.inc();
                log.error("Error copying batch", e);
                dst.rollback();
                throw e;
            }
        }
    }

    private void ensureProgressTable(Connection dst) throws Exception {
        log.debug("Ensuring progress table exists");
        try (Statement st = dst.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS migration_progress (task_name text primary key, last_id text)");
        }
    }

    private String loadProgress(Connection dst, String task) throws Exception {
        log.debug("Loading progress for task {}", task);
        try (Statement st = dst.createStatement(); ResultSet rs = st.executeQuery("SELECT last_id FROM migration_progress WHERE task_name='" + task + "'")) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    private void saveProgress(Connection dst, String task, String lastId) throws Exception {
        log.debug("Saving progress for task {}: {}", task, lastId);
        dst.setAutoCommit(false);
        try (Statement st = dst.createStatement()) {
            int updated = st.executeUpdate("UPDATE migration_progress SET last_id='" + lastId + "' WHERE task_name='" + task + "'");
            if (updated == 0) {
                st.executeUpdate("INSERT INTO migration_progress(task_name, last_id) VALUES('" + task + "', '" + lastId + "')");
            }
        }
        dst.commit();
    }
}
