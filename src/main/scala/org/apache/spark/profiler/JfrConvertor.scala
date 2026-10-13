/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.profiler

import java.util.{HashMap => JHashMap, TreeMap => JTreeMap}

import scala.collection.mutable.ArrayBuffer

import one.convert.{Arguments, Filter, ProfilerJfrConverter}
import one.jfr.ProfilerJfrReader
import one.jfr.event.Event
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.profiler.ui.{ExecutorProfilerFilter, ProfilerFilter, ProfilerSampleType, TaskProfilerFilter}
import org.apache.spark.util.Utils

/** Resolves Java thread IDs per chunk and converts selected samples in a single pass. */
private[profiler] class JfrConvertor(
    reader: ProfilerJfrReader,
    executorId: String,
    selection: ProfilerFilter,
    tasks: Seq[SparkProfilerTaskThreadEvent],
    root: FlamegraphNode)
  extends ProfilerJfrConverter(reader, if (selection.sampleType == ProfilerSampleType.NativeMemory) {
    new Arguments("--nativemem", "--leak")
  } else {
    new Arguments(s"--${selection.sampleType}")
  }) {

  private val tasksByThread = tasks.filter(_.executorId == executorId).groupBy(_.javaThreadId)

  override protected def collectEvents(filter: Filter): Unit = {
    super.collectEvents(eventFilter())
  }

  private def eventFilter(): Filter = selection match {
    case executor: ExecutorProfilerFilter => executorProfilerEventFilter(executor)
    case _: TaskProfilerFilter => taskProfilerEventFilter()
  }

  private def executorProfilerEventFilter(executor: ExecutorProfilerFilter): Filter = {
    if (executor.executor.exists(_ != executorId)) {
      JfrConvertor.intervalFilter(Nil)
    } else if (executor.threads.isEmpty &&
        executor.startTime.isEmpty && executor.endTime.isEmpty) {
      null
    } else {
      val intervals = new ArrayBuffer[(Int, Long, Long)]
      val start = executor.startTime.map(_ * 1000000L).getOrElse(Long.MinValue)
      val end = executor.endTime.map(_ * 1000000L).getOrElse(Long.MaxValue)
      jfr.threads.forEach { (threadId: Long, name: String) =>
        val javaThreadId = jfr.javaThreads.get(threadId)
        if (executor.threads.isEmpty || (javaThreadId != null &&
            executor.threads.contains(s"$executorId:$javaThreadId"))) {
          intervals += ((threadId.toInt, start, end))
        }
      }
      JfrConvertor.intervalFilter(intervals.toSeq)
    }
  }

  private def taskProfilerEventFilter(): Filter = {
    val intervals = new ArrayBuffer[(Int, Long, Long)]
    // Spark's status store has already resolved SQL/job/stage/task selections.
    jfr.javaThreads.forEach { (threadId: Long, javaThreadId: java.lang.Long) =>
      tasksByThread.getOrElse(javaThreadId.longValue(), Nil).foreach { task =>
        val endMillis = if (task.endTime == task.startTime) task.endTime + 1 else task.endTime
        val start = math.max(task.startTime * 1000000L, jfr.chunkStartNanos)
        val end = math.min(endMillis * 1000000L, jfr.chunkEndNanos)
        if (end > start) intervals += ((threadId.toInt, start, end))
      }
    }
    JfrConvertor.intervalFilter(intervals.toSeq)
  }

  override protected def convertChunk(): Unit = {
    collector.forEach { (event: Event, samples: Long, value: Long) =>
      val stack = jfr.stackTraces.get(event.stackTraceId)
      val frames = if (stack == null) {
        Seq("[unknown]")
      } else {
        stack.methods.indices.reverse.map(i => getMethodName(stack.methods(i), stack.types(i)))
      }
      root.addStack(frames, samples)
    }
  }
}

private[profiler] object JfrConvertor extends Logging {
  /** The intervals use epoch nanoseconds, not ticks local to a particular JFR chunk. */
  def intervalFilter(intervals: Seq[(Int, Long, Long)]): Filter = {
    val deltas = new JHashMap[Integer, JTreeMap[java.lang.Long, Integer]]()
    intervals.foreach { case (threadId, start, end) =>
      require(end > start, "A profiling interval must have positive duration")
      val times = deltas.computeIfAbsent(threadId, _ =>
        new JTreeMap[java.lang.Long, Integer]())
      def add(time: Long, delta: Int): Unit = {
        times.put(time, Option(times.get(time)).fold(delta)(_.intValue() + delta))
      }
      add(start, 1)
      add(end, -1)
    }
    new Filter(deltas)
  }

  def convert(
      fs: FileSystem,
      path: Path,
      executorId: String,
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent],
      root: FlamegraphNode): Unit = {
    val size = fs.getFileStatus(path).getLen
    val input = fs.open(path)
    val channel = Utils.tryWithSafeFinallyAndFailureCallbacks {
      new FSDataInputStreamChannel(input, size)
    }(catchBlock = input.close())
    // Own the channel here so reader initialization failures also close the input.
    Utils.tryWithResource(channel) { channel =>
      val reader = new ProfilerJfrReader(channel)
      new JfrConvertor(reader, executorId, selection, tasks, root).convert()
      if (reader.incomplete()) {
        logWarning("The JFR file is incomplete; using partial profiling data. " +
          "Retry after the next profiler DFS upload for more complete results.")
      }
    }
  }
}
