package com.example.migrator;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigTest {
    @Test
    void fromLoadsIdsFromFile() throws Exception {
        Path tmp = Files.createTempFile("ids", ".txt");
        Files.write(tmp, List.of("id1", "", "id2"));

        Properties props = new Properties();
        props.setProperty("source.url", "surl");
        props.setProperty("source.user", "suser");
        props.setProperty("source.password", "spass");
        props.setProperty("target.url", "turl");
        props.setProperty("target.user", "tuser");
        props.setProperty("target.password", "tpass");
        props.setProperty("batchSize", "50");
        props.setProperty("taskName", "task1");
        props.setProperty("idsFile", tmp.toString());
        props.setProperty("impl", "copy");

        Config cfg = Config.from(props);
        assertEquals("surl", cfg.sourceUrl());
        assertEquals("tuser", cfg.targetUser());
        assertEquals(50, cfg.batchSize());
        assertEquals(List.of("id1", "id2"), cfg.ids());
        assertEquals("copy", cfg.impl());
    }
}
