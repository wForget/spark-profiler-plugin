/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

package one.convert;

import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class ProfilerFilterTest {
    @Test
    public void timeRangeUsesHalfOpenFilterAndInclusiveChunkEnd() {
        ProfilerFilter filter = new ProfilerFilter(100, 200);
        assertFalse(filter.overlaps(0, 99));
        assertTrue(filter.overlaps(0, 100));
        assertTrue(filter.overlaps(100, 100));
        assertTrue(filter.overlaps(150, 250));
        assertTrue(filter.overlaps(0, 300));
        assertFalse(filter.overlaps(200, 300));
        assertTrue(filter.matches(1, null, 100));
        assertFalse(filter.matches(1, null, 200));
    }

    @Test
    public void boundsIncludeBothThreadIdNamespacesAndAllIntervals() {
        ProfilerFilter filter = new ProfilerFilter(
                Map.of(1, new TreeMap<>(Map.of(100L, 1, 200L, -1, 400L, 1, 500L, -1))),
                Map.of(2L, range(50, 600)));
        assertFalse(filter.overlaps(0, 49));
        assertTrue(filter.overlaps(0, 50));
        assertTrue(filter.overlaps(550, 599));
        assertFalse(filter.overlaps(600, 700));
        assertTrue(filter.matches(3, 2L, 550));
        assertTrue(filter.matches(1, null, 450));
        assertFalse(filter.matches(1, null, 300));
    }

    @Test
    public void gapsWithinGlobalBoundsAreConservativelyRetained() {
        ProfilerFilter filter = new ProfilerFilter(
                Map.of(1, range(100, 200), 2, range(400, 500)), Map.of());
        assertTrue(filter.overlaps(250, 350));
        assertFalse(filter.matches(1, null, 300));
        assertFalse(filter.matches(2, null, 300));
    }

    @Test
    public void emptyFiltersNeverOverlap() {
        assertFalse(new ProfilerFilter(Map.of(), Map.of())
                .overlaps(Long.MIN_VALUE, Long.MAX_VALUE));
        assertFalse(new ProfilerFilter(Map.of(1, new TreeMap<>()), Map.of())
                .overlaps(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void unboundedIntervalsDoNotOverflow() {
        ProfilerFilter filter = new ProfilerFilter(
                Map.of(), Map.of(1L, range(Long.MIN_VALUE, Long.MAX_VALUE)));
        assertTrue(filter.overlaps(Long.MIN_VALUE, Long.MIN_VALUE));
        assertTrue(filter.overlaps(-1, 1));
        assertTrue(filter.overlaps(Long.MAX_VALUE - 1, Long.MAX_VALUE));
        assertFalse(filter.overlaps(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    private static TreeMap<Long, Integer> range(long start, long end) {
        return new TreeMap<>(Map.of(start, 1, end, -1));
    }
}
