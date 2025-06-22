package com.example.migrator;

import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ConfigurationProperties(prefix = "migration")
public record Config(
        String sourceUrl,
        String sourceUser,
        String sourcePassword,
        String targetUrl,
        String targetUser,
        String targetPassword,
        int batchSize,
        String taskName,
        List<String> ids) {

    public static Config from(Properties props) {
        String idsFile = props.getProperty("idsFile");
        List<String> ids = null;
        if (idsFile != null) {
            try (var reader = new java.io.BufferedReader(new java.io.FileReader(idsFile))) {
                ids = reader.lines().filter(s -> !s.isBlank()).collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Failed to read ids file", e);
            }
        }
        return new Config(
                props.getProperty("source.url"),
                props.getProperty("source.user"),
                props.getProperty("source.password"),
                props.getProperty("target.url"),
                props.getProperty("target.user"),
                props.getProperty("target.password"),
                Integer.parseInt(props.getProperty("batchSize", "1000")),
                props.getProperty("taskName", "default"),
                ids
        );
    }

    public static Config from(Environment env) {
        String idsFile = env.getProperty("migration.idsFile");
        List<String> ids = null;
        if (idsFile != null) {
            try (var reader = new java.io.BufferedReader(new java.io.FileReader(idsFile))) {
                ids = reader.lines().filter(s -> !s.isBlank()).collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Failed to read ids file", e);
            }
        }
        return new Config(
                env.getProperty("migration.source.url"),
                env.getProperty("migration.source.user"),
                env.getProperty("migration.source.password"),
                env.getProperty("migration.target.url"),
                env.getProperty("migration.target.user"),
                env.getProperty("migration.target.password"),
                Integer.parseInt(env.getProperty("migration.batchSize", "1000")),
                env.getProperty("migration.taskName", "default"),
                ids
        );
    }
}
