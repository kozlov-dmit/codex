package com.example.migrator;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

public class MigrationServiceProgressTest {
    @Test
    void progressIsSavedAndLoaded() throws Exception {
        Config cfg = new Config(
                "jdbc:h2:mem:src", "", "",
                "jdbc:h2:mem:dst", "", "",
                10, "task", null
        );
        MigrationService service = new MigrationService(cfg);

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:dst")) {
            Method ensure = MigrationService.class.getDeclaredMethod("ensureProgressTable", Connection.class);
            ensure.setAccessible(true);
            ensure.invoke(service, conn);

            Method save = MigrationService.class.getDeclaredMethod("saveProgress", Connection.class, String.class, String.class);
            save.setAccessible(true);
            save.invoke(service, conn, "task", "1");

            Method load = MigrationService.class.getDeclaredMethod("loadProgress", Connection.class, String.class);
            load.setAccessible(true);
            String last = (String) load.invoke(service, conn, "task");
            assertEquals("1", last);

            save.invoke(service, conn, "task", "2");
            last = (String) load.invoke(service, conn, "task");
            assertEquals("2", last);
        }
    }
}
