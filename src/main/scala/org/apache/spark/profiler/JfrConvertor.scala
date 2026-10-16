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

import scala.collection.mutable

import one.convert.{Arguments, ProfilerFilter => Filter, ProfilerJfrConverter}
import one.jfr.ProfilerJfrReader
import one.jfr.event.Event
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.profiler.ui.{ExecutorProfilerFilter, ProfilerFilter, ProfilerSampleType, TaskProfilerFilter}
import org.apache.spark.util.Utils

/** Builds thread filters once and resolves sample Java thread IDs in a single pass. */
private[profiler] class JfrConvertor(
    reader: ProfilerJfrReader,
    executorId: String,
    selection: ProfilerFilter,
    tasks: Seq[SparkProfilerTaskThreadEvent],
    root: FlamegraphNode)
  extends ProfilerJfrConverter(reader, if (selection.sampleType == ProfilerSampleType.NativeMemory) {
    new Arguments("--nativemem", "--leak", "--norm")
  } else {
    new Arguments(s"--${selection.sampleType}", "--norm")
  }) {

  override def readFilter(): Filter = selection match {
    case executor: ExecutorProfilerFilter => executorProfilerEventFilter(executor)
    case _: TaskProfilerFilter => taskProfilerEventFilter()
  }

  private def executorProfilerEventFilter(executor: ExecutorProfilerFilter): Filter = {
    if (executor.executor.exists(_ != executorId)) {
      JfrConvertor.intervalFilter()
    } else if (executor.threads.isEmpty &&
        executor.startTime.isEmpty && executor.endTime.isEmpty) {
      null
    } else {
      val start = executor.startTime.map(_ * 1000000L).getOrElse(Long.MinValue)
      val end = executor.endTime.map(_ * 1000000L).getOrElse(Long.MaxValue)
      if (executor.threads.isEmpty) {
        new Filter(start, end)
      } else {
        val prefix = s"$executorId:"
        val intervals = executor.threads.toSeq.filter(_.startsWith(prefix)).map { thread =>
          (thread.substring(prefix.length).toLong, start, end)
        }
        JfrConvertor.intervalFilter(tidIntervals = intervals)
      }
    }
  }

  private def taskProfilerEventFilter(): Filter = {
    // Spark's status store has already resolved SQL/job/stage/task selections.
    val intervals = tasks.filter(_.executorId == executorId).flatMap { task =>
      val endMillis = if (task.endTime == task.startTime) task.endTime + 1 else task.endTime
      val start = task.startTime * 1000000L
      val end = endMillis * 1000000L
      Option.when(end > start)((task.javaThreadId, start, end))
    }
    JfrConvertor.intervalFilter(javaThreadIntervals = intervals)
  }

  override protected def convertChunk(): Unit = {
    collector.forEach { (event: Event, samples: Long, value: Long) =>
      val stack = jfr.stackTraces.get(event.stackTraceId)
      val frames = if (stack == null) {
        Seq("[unknown]")
      } else {
        // Resolve names lazily: a tree budget may discard the remaining stack suffix.
        stack.methods.indices.reverse.view.map(i => getMethodName(stack.methods(i), stack.types(i)))
      }
      root.addStack(frames, samples)
    }
  }
}

private[profiler] object JfrConvertor extends Logging {
  /** Collect JFR thread IDs from every complete chunk, including native-only threads. */
  def listThreads(fs: FileSystem, path: Path): Seq[(Long, String)] = {
    withReader(fs, path) { reader =>
      val threads = mutable.Map.empty[Long, String]
      var more = true
      while (more) {
        reader.threads.forEach { (tid: Long, name: String) =>
          threads(tid) = Option(name).getOrElse("")
        }
        more = reader.nextChunk()
      }
      threads.toSeq.sortBy(_._1)
    }
  }

  /** The intervals use epoch nanoseconds, not ticks local to a particular JFR chunk. */
  def intervalFilter(
      tidIntervals: Seq[(Long, Long, Long)] = Nil,
      javaThreadIntervals: Seq[(Long, Long, Long)] = Nil): Filter = {
    new Filter(intervalDeltas(tidIntervals), intervalDeltas(javaThreadIntervals))
  }

  private def intervalDeltas(
      intervals: Seq[(Long, Long, Long)]): JHashMap[java.lang.Long,
        JTreeMap[java.lang.Long, Integer]] = {
    val deltas = new JHashMap[java.lang.Long, JTreeMap[java.lang.Long, Integer]]()
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
    deltas
  }

  def convert(
      fs: FileSystem,
      path: Path,
      executorId: String,
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent],
      root: FlamegraphNode): Unit = {
    withReader(fs, path) { reader =>
      new JfrConvertor(reader, executorId, selection, tasks, root).convert()
    }
  }

  private def withReader[T](fs: FileSystem, path: Path)(read: ProfilerJfrReader => T): T = {
    val size = fs.getFileStatus(path).getLen
    val input = fs.open(path)
    val channel = Utils.tryWithSafeFinallyAndFailureCallbacks {
      new FSDataInputStreamChannel(input, size)
    }(catchBlock = input.close())
    // Own the channel here so reader initialization failures also close the input.
    Utils.tryWithResource(channel) { channel =>
      val reader = new ProfilerJfrReader(channel)
      val result = read(reader)
      if (reader.incomplete()) {
        logWarning("The JFR file is incomplete; using partial profiling data. " +
          "Retry after the next profiler DFS upload for more complete results.")
      }
      result
    }
  }
}
