package com.example.migrator;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
}
