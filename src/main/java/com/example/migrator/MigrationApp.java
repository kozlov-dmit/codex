package com.example.migrator;

import io.prometheus.client.exporter.HTTPServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class MigrationApp implements CommandLineRunner {

    private final MigrationService service;

    public MigrationApp(MigrationService service) {
        this.service = service;
    }

    @Bean
    Config config(Environment env) {
        return Config.from(env);
    }

    @Bean
    MigrationService migrationService(Config config) {
        return new MigrationService(config);
    }

    public static void main(String[] args) {
        SpringApplication.run(MigrationApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        HTTPServer server = new HTTPServer(9090);
        try {
            service.run();
        } finally {
            server.close();
        }
    }
}
