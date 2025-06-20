package com.example.migrator;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import io.prometheus.client.CollectorRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class MigrationServiceEmbeddedPostgresTest {

    @Test
    void migrationCopiesRows() throws Exception {
        try (EmbeddedPostgres src = EmbeddedPostgres.builder().start();
             EmbeddedPostgres dst = EmbeddedPostgres.builder().start()) {

            String srcUrl = src.getJdbcUrl("postgres", "postgres");
            String dstUrl = dst.getJdbcUrl("postgres", "postgres");

            try (Connection c = DriverManager.getConnection(srcUrl, "postgres", "postgres");
                 Statement st = c.createStatement()) {
                st.execute("CREATE TABLE person(id text primary key, birthday date)");
                st.execute("INSERT INTO person(id, birthday) VALUES('1', current_date - interval '10 years')");
                st.execute("INSERT INTO person(id, birthday) VALUES('2', current_date - interval '20 years')");
                st.execute("INSERT INTO person(id, birthday) VALUES('3', current_date - interval '5 years')");
            }
            try (Connection c = DriverManager.getConnection(dstUrl, "postgres", "postgres");
                 Statement st = c.createStatement()) {
                st.execute("CREATE TABLE kids(id text primary key, birthday date)");
            }

            Config cfg = new Config(srcUrl, "postgres", "postgres", dstUrl, "postgres", "postgres", 10, "task", null);
            MigrationService service = new MigrationService(cfg);
            service.run();
            Double processed = CollectorRegistry.defaultRegistry.getSampleValue("migrator_processed_total");
            assertEquals(2.0, processed);

            try (Connection c = DriverManager.getConnection(dstUrl, "postgres", "postgres");
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM kids")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }
}
