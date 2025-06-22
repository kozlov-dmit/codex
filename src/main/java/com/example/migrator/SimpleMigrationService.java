package com.example.migrator;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SimpleMigrationService implements MigrationService {
    private static final Logger log = LoggerFactory.getLogger(SimpleMigrationService.class);

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

    public SimpleMigrationService(Config config) {
        this.config = config;
    }

    @Override
    public void run() throws Exception {
        log.info("Starting migration task {}", config.taskName());
        long start = System.nanoTime();
        try (Connection src = DriverManager.getConnection(config.sourceUrl(), config.sourceUser(), config.sourcePassword());
             Connection dst = DriverManager.getConnection(config.targetUrl(), config.targetUser(), config.targetPassword())) {

            ensureProgressTable(dst);
            String lastId = loadProgress(dst, config.taskName());
            List<String> ids = config.ids();
            if (ids == null) {
                ids = fetchIds(src, lastId);
            } else if (lastId != null) {
                ids = ids.subList(ids.indexOf(lastId) + 1, ids.size());
            }

            log.info("Total ids to process: {}", ids.size());
            processIds(src, dst, ids, start);
        }
        log.info("Migration finished. Total processed {}", (int) processedCounter.get());
    }

    private List<String> fetchIds(Connection src, String lastId) throws Exception {
        List<String> ids = new ArrayList<>();
        String query = "SELECT id FROM person WHERE birthDay > current_date - interval '18 years'" +
                (lastId != null ? " AND id > '" + lastId + "'" : "") +
                " ORDER BY id";
        try (Statement st = src.createStatement(); ResultSet rs = st.executeQuery(query)) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        }
        return ids;
    }

    private void processIds(Connection src, Connection dst, List<String> ids, long start) throws Exception {
        if (ids.isEmpty()) {
            return;
        }
        int batchSize = config.batchSize();
        List<String> batch = new ArrayList<>(batchSize);
        int processed = 0;
        for (String id : ids) {
            batch.add(id);
            if (batch.size() >= batchSize) {
                transferBatch(src, dst, batch);
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
            transferBatch(src, dst, batch);
            processedCounter.inc(batch.size());
            processed += batch.size();
            double duration = (System.nanoTime() - start) / 1_000_000_000.0;
            double speed = processed / duration;
            speedGauge.set(speed);
            log.info("Processed {} of {} ids ({} recs/sec)", processed, ids.size(), String.format("%.2f", speed));
            saveProgress(dst, config.taskName(), batch.get(batch.size() - 1));
        }
    }

    private void transferBatch(Connection src, Connection dst, List<String> batch) throws Exception {
        String inList = batch.stream().map(id -> "'" + id + "'").reduce((a, b) -> a + "," + b).orElse("'0'");
        String sql = "SELECT id, birthday FROM person WHERE id IN (" + inList + ")";
        try (Statement st = src.createStatement(); ResultSet rs = st.executeQuery(sql);
             PreparedStatement ps = dst.prepareStatement("INSERT INTO kids(id, birthday) VALUES (?, ?)");) {
            dst.setAutoCommit(false);
            try {
                while (rs.next()) {
                    ps.setString(1, rs.getString(1));
                    ps.setDate(2, rs.getDate(2));
                    ps.addBatch();
                }
                ps.executeBatch();
                dst.commit();
            } catch (Exception e) {
                errorCounter.inc();
                dst.rollback();
                throw e;
            }
        }
    }

    private void ensureProgressTable(Connection dst) throws Exception {
        try (Statement st = dst.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS migration_progress (task_name text primary key, last_id text)");
        }
    }

    private String loadProgress(Connection dst, String task) throws Exception {
        try (Statement st = dst.createStatement(); ResultSet rs = st.executeQuery("SELECT last_id FROM migration_progress WHERE task_name='" + task + "'");) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    private void saveProgress(Connection dst, String task, String lastId) throws Exception {
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
