package com.example.migrator;

import io.prometheus.client.exporter.HTTPServer;
import java.io.*;
import java.sql.*;
import java.util.*;

public class MigrationApp {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream in = MigrationApp.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) {
                System.err.println("config.properties not found");
                return;
            }
            props.load(in);
        }

        Config config = Config.from(props);
        MigrationService service = new MigrationService(config);
        HTTPServer server = new HTTPServer(9090);
        try {
            service.run();
        } finally {
            server.close();
        }
    }
}
