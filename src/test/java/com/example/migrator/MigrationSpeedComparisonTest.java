package com.example.migrator;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import io.prometheus.client.CollectorRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MigrationSpeedComparisonTest {
    private static final String LOCALE = "C";

    @Test
    void copyShouldBeFasterThanSimple() throws Exception {
        try (EmbeddedPostgres src = EmbeddedPostgres.builder().setLocaleConfig("locale", LOCALE).start();
             EmbeddedPostgres dst = EmbeddedPostgres.builder().setLocaleConfig("locale", LOCALE).start()) {

            String srcUrl = src.getJdbcUrl("postgres", "postgres");
            String dstUrl = dst.getJdbcUrl("postgres", "postgres");

            try (Connection c = DriverManager.getConnection(srcUrl, "postgres", "postgres");
                 Statement st = c.createStatement()) {
                st.execute("CREATE TABLE person(id text primary key, birthday date)");
                for (int i = 1; i <= 2000; i++) {
                    st.execute("INSERT INTO person(id, birthday) VALUES('" + i + "', current_date - interval '10 years')");
                }
            }
            try (Connection c = DriverManager.getConnection(dstUrl, "postgres", "postgres");
                 Statement st = c.createStatement()) {
                st.execute("CREATE TABLE kids(id text primary key, birthday date)");
            }

            Config cfgCopy = new Config(srcUrl, "postgres", "postgres", dstUrl, "postgres", "postgres", 500, "copyTask", null, "copy");
            CollectorRegistry.defaultRegistry.clear();
            long start = System.currentTimeMillis();
            new CopyMigrationService(cfgCopy).run();
            long copyTime = System.currentTimeMillis() - start;

            try (Connection c = DriverManager.getConnection(dstUrl, "postgres", "postgres");
                 Statement st = c.createStatement()) {
                st.execute("TRUNCATE kids");
                st.execute("DELETE FROM migration_progress");
            }

            Config cfgSimple = new Config(srcUrl, "postgres", "postgres", dstUrl, "postgres", "postgres", 500, "simpleTask", null, "simple");
            CollectorRegistry.defaultRegistry.clear();
            start = System.currentTimeMillis();
            new SimpleMigrationService(cfgSimple).run();
            long simpleTime = System.currentTimeMillis() - start;

            assertTrue(simpleTime > copyTime);
        }
    }
}
