/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

package one.convert;

import jdk.jfr.Event;
import jdk.jfr.Recording;
import one.jfr.ProfilerJfrReader;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ProfilerJfrConverterTest {
    private static byte[] recording;

    public static class Marker extends Event {
    }

    @BeforeClass
    public static void createRecording() throws IOException {
        Path path = Files.createTempFile("profiler-chunk-skip", ".jfr");
        try {
            try (Recording jfr = new Recording()) {
                jfr.enable(Marker.class);
                jfr.start();
                new Marker().commit();
                jfr.stop();
                jfr.dump(path);
            }
            byte[] chunk = Files.readAllBytes(path);
            assertEquals("Fixture must contain one chunk", chunk.length,
                    ByteBuffer.wrap(chunk).getLong(8));
            ByteBuffer chunks = ByteBuffer.allocate(chunk.length * 3);
            for (long start : new long[]{100, 200, 300}) {
                // Use deterministic epoch times independent of each chunk's tick clock.
                ByteBuffer.wrap(chunk).putLong(32, start).putLong(40, 10);
                chunks.put(chunk);
            }
            recording = chunks.array();
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    public void skipsChunksBeforeAndAfterFilterWithoutDecodingTheirEvents() throws IOException {
        assertChunks(new ProfilerFilter(200, 210), List.of(200L), 2);
        assertChunks(new ProfilerFilter(110, 200), List.of(100L), 2);
        assertChunks(new ProfilerFilter(400, 500), List.of(), 3);
    }

    @Test
    public void emptyFilterSkipsAllChunksAndAbsentFilterKeepsAllChunks() throws IOException {
        assertChunks(new ProfilerFilter(Map.of(), Map.of()), List.of(), 3);
        assertChunks(null, List.of(100L, 200L, 300L), 0);
    }

    private void assertChunks(ProfilerFilter filter, List<Long> expected, int skipped)
            throws IOException {
        try (CountingReader reader = new CountingReader()) {
            List<Long> decoded = new ArrayList<>();
            ProfilerJfrConverter converter = new ProfilerJfrConverter(reader, new Arguments()) {
                @Override
                protected ProfilerFilter readFilter() {
                    return filter;
                }

                @Override
                protected void collectEvents(ProfilerFilter selected) throws IOException {
                    decoded.add(jfr.chunkStartNanos);
                    super.collectEvents(selected);
                }
            };
            converter.convert();
            assertEquals(expected, decoded);
            assertEquals(skipped, reader.skipped);
            assertTrue(reader.eof());
            assertFalse(reader.incomplete());
        }
    }

    private static class CountingReader extends ProfilerJfrReader {
        int skipped;

        CountingReader() throws IOException {
            super(ByteBuffer.wrap(recording));
        }

        @Override
        public boolean skipChunk() throws IOException {
            skipped++;
            return super.skipChunk();
        }
    }
}
