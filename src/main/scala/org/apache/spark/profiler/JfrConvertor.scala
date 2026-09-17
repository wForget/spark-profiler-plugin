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

import java.io.{File, IOException}
import java.util.{HashMap => JHashMap, TreeMap => JTreeMap}

import scala.collection.mutable.ArrayBuffer

import one.convert.{Arguments, Filter, JfrConverter}
import one.jfr.JfrReader
import one.jfr.event.{Event, ExecutionSample, SpanEvent}

import org.apache.spark.profiler.ui.{ProfilerFilter, ProfilerIndex, ProfilerThread, TaskSpan}
import org.apache.spark.util.Utils

/** Converts each JFR chunk before its stack and method dictionaries are replaced. */
private[profiler] class JfrConvertor(
    reader: JfrReader,
    sampleType: String,
    eventFilter: Filter,
    root: FlamegraphNode)
  extends JfrConverter(reader, new Arguments(s"--$sampleType")) {

  override protected def collectEvents(filter: Filter): Unit = {
    // Arguments only select the sample type. Selection intervals replace the CLI span filter.
    super.collectEvents(eventFilter)
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

private[profiler] object JfrConvertor {
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

  def eventFilter(
      executorId: String,
      index: ProfilerIndex,
      selection: ProfilerFilter): Filter = {
    if (selection.mode == "task") {
      intervalFilter(index.selectedSpans(selection).filter(_.executorId == executorId)
        .map(s => (s.threadId, s.startNanos, s.endNanos)))
    } else if (selection.executor.exists(_ != executorId)) {
      intervalFilter(Nil)
    } else if (selection.threads.nonEmpty ||
        selection.startTime.isDefined || selection.endTime.isDefined) {
      val start = selection.startTime.map(_ * 1000000L).getOrElse(Long.MinValue)
      val end = selection.endTime.map(_ * 1000000L).getOrElse(Long.MaxValue)
      intervalFilter(index.threadOptions(selection)
        .filter(t => t.executorId == executorId &&
          (selection.threads.isEmpty || selection.threads.contains(t.id)))
        .map(t => (t.threadId, start, end)))
    } else {
      null
    }
  }

  def scan(file: File, executorId: String): (Seq[ProfilerThread], Seq[TaskSpan]) = {
    val threads = new scala.collection.mutable.HashMap[(Int, String), ProfilerThread]
    val spans = new ArrayBuffer[TaskSpan]
    Utils.tryWithResource(new JfrReader(file.getAbsolutePath)) { reader =>
      reader.stopAtNewChunk = true
      while (reader.hasMoreChunks()) {
        var event = reader.readEvent()
        while (event != null) {
          event match {
            case sample: ExecutionSample
                if sample.threadState != ExecutionSample.CPU_TIME_SAMPLE =>
              val state = reader.getEnumValue("jdk.types.ThreadState", sample.threadState)
              if (state != null) {
                val sampleType = if (state == "STATE_DEFAULT") "cpu" else "wall"
                val name = Option(reader.threads.get(sample.tid))
                  .getOrElse(s"Thread ${sample.tid}")
                val time = reader.eventTimeToNanos(sample.time) / 1000000L
                val previous = threads.get((sample.tid, sampleType))
                threads((sample.tid, sampleType)) =
                  ProfilerThread(executorId, sample.tid, name, sampleType,
                    previous.fold(time)(t => math.min(t.startTime, time)),
                    previous.fold(time + 1)(t => math.max(t.endTime, time + 1)))
              }
            case span: SpanEvent =>
              TaskSpan.parse(span.tag, executorId, span.tid,
                reader.eventTimeToNanos(span.time),
                reader.eventTimeToNanos(span.time + span.duration)).foreach(spans += _)
            case _ =>
          }
          event = reader.readEvent()
        }
      }
      requireComplete(reader)
    }
    (threads.values.toSeq, spans.toSeq)
  }

  def convert(
      file: File,
      sampleType: String,
      filter: Filter,
      root: FlamegraphNode): Unit = {
    Utils.tryWithResource(new JfrReader(file.getAbsolutePath)) { reader =>
      new JfrConvertor(reader, sampleType, filter, root).convert()
      requireComplete(reader)
    }
  }

  private def requireComplete(reader: JfrReader): Unit = {
    if (reader.incomplete()) {
      throw new IOException("The JFR file is incomplete; retry after the next profiler DFS upload.")
    }
  }
}
