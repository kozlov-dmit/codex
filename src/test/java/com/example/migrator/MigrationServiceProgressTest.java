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
                10, "task", null, "copy"
        );
        CopyMigrationService service = new CopyMigrationService(cfg);

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:dst")) {
            Method ensure = CopyMigrationService.class.getDeclaredMethod("ensureProgressTable", Connection.class);
            ensure.setAccessible(true);
            ensure.invoke(service, conn);

            Method save = CopyMigrationService.class.getDeclaredMethod("saveProgress", Connection.class, String.class, String.class);
            save.setAccessible(true);
            save.invoke(service, conn, "task", "1");

            Method load = CopyMigrationService.class.getDeclaredMethod("loadProgress", Connection.class, String.class);
            load.setAccessible(true);
            String last = (String) load.invoke(service, conn, "task");
            assertEquals("1", last);

            save.invoke(service, conn, "task", "2");
            last = (String) load.invoke(service, conn, "task");
            assertEquals("2", last);
        }
    }
}
