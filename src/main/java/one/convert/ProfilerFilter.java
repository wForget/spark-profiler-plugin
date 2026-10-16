/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

package one.convert;

import one.jfr.Dictionary;

import java.util.Map;
import java.util.TreeMap;

public final class ProfilerFilter {
    // Per-thread timelines, keyed by JFR thread id and Java thread id respectively.
    private final Dictionary<Intervals> intervals;
    private final Dictionary<Intervals> javaThreadIntervals;
    private final Intervals allThreads;

    public ProfilerFilter(Map<? extends Number, TreeMap<Long, Integer>> deltas,
                          Map<Long, TreeMap<Long, Integer>> javaThreadDeltas) {
        this.intervals = new Dictionary<>(deltas.size());
        deltas.forEach((tid, threadDeltas) -> intervals.put(tid.longValue(), new Intervals(threadDeltas)));
        this.javaThreadIntervals = new Dictionary<>(javaThreadDeltas.size());
        javaThreadDeltas.forEach((tid, threadDeltas) ->
                javaThreadIntervals.put(tid, new Intervals(threadDeltas)));
        this.allThreads = null;
    }

    /** Apply a time range to all threads, including threads first seen in later chunks. */
    public ProfilerFilter(long start, long end) {
        if (end <= start) {
            throw new IllegalArgumentException("A profiling interval must have positive duration");
        }
        this.intervals = new Dictionary<>();
        this.javaThreadIntervals = new Dictionary<>();
        this.allThreads = new Intervals(new TreeMap<>(Map.of(start, 1, end, -1)));
    }

    public boolean matches(int tid, Long javaThreadId, long time) {
        return matches(allThreads, time) || matches(intervals.get(tid), time)
                || (javaThreadId != null && javaThreadId > 0
                    && matches(javaThreadIntervals.get(javaThreadId), time));
    }

    public boolean contains(long tid, Long javaThreadId) {
        return allThreads != null || intervals.get(tid) != null
                || (javaThreadId != null && javaThreadId > 0
                    && javaThreadIntervals.get(javaThreadId) != null);
    }

    private static boolean matches(Intervals intervals, long time) {
        return intervals != null && intervals.enclosing(time) > 0;
    }

    // Sort all starts and ends and count the number of open intervals (depth) at each boundary
    private static final class Intervals {
        private final long[] bounds;
        private final int[] depth;

        Intervals(TreeMap<Long, Integer> deltas) {
            long[] bounds = new long[deltas.size()];
            int[] depth = new int[deltas.size()];

            int i = 0;
            int open = 0;
            for (Map.Entry<Long, Integer> entry : deltas.entrySet()) {
                bounds[i] = entry.getKey();
                depth[i] = open += entry.getValue();
                i++;
            }

            this.bounds = bounds;
            this.depth = depth;
        }

        // Returns how many intervals enclose the given point
        int enclosing(long point) {
            int lo = 0;
            int hi = bounds.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (bounds[mid] <= point) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo > 0 ? depth[lo - 1] : 0;
        }
    }
}
