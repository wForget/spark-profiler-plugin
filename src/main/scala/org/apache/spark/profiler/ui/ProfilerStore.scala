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

package org.apache.spark.profiler.ui

import java.io.{BufferedWriter, Writer}
import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.profiler.{FlamegraphNode, JfrConvertor, PROFILER_FLAMEGRAPH_MAX_DEPTH, PROFILER_FLAMEGRAPH_MAX_NODES, PROFILER_FLAMEGRAPH_MIN_FRACTION, PROFILER_JFR_PARSE_THREADS, SparkProfilerTaskThreadEvent}
import org.apache.spark.util.{ThreadUtils, Utils}

private[profiler] case class ProfilerThread(executorId: String, javaThreadId: Long, name: String) {
  def id: String = s"$executorId:$javaThreadId"
}

/** Reads selected recordings directly from the configured file system. */
private[profiler] class ProfilerStore(
    conf: SparkConf,
    directory: String,
    appId: String,
    attemptId: Option[String]) extends Logging {

  private val appPath = new Path(directory, Utils.nameForAppAndAttempt(appId, attemptId))
  private val executorFile = "profile-exec-(.+)\\.jfr".r

  private val minFraction = conf.get(PROFILER_FLAMEGRAPH_MIN_FRACTION)
  private val maxNodes = conf.get(PROFILER_FLAMEGRAPH_MAX_NODES)
  private val maxDepth = conf.get(PROFILER_FLAMEGRAPH_MAX_DEPTH)
  private val parseThreads = conf.get(PROFILER_JFR_PARSE_THREADS)

  def listThreads(executor: Option[String]): Seq[ProfilerThread] = synchronized {
    executor.toSeq.flatMap { id =>
      val fs = appPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
      recordings(fs).filter(_._1 == id).flatMap { case (_, path) =>
        JfrConvertor.listThreads(fs, path).map { case (javaThreadId, name) =>
          ProfilerThread(id, javaThreadId, name)
        }
      }
    }
  }

  // Keep the request lock through output: another request must not build additional trees
  // while the first request still holds its result during serialization or a slow response write.
  def writeFlamegraph(
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent],
      writer: Writer): Unit = synchronized {
    val root = flamegraph(selection, tasks)
    val buffered = new BufferedWriter(writer, 16 * 1024)
    root.writeJson(buffered, minFraction)
    buffered.flush()
  }

  private def recordings(fs: FileSystem): Seq[(String, Path)] = {
    if (fs.exists(appPath)) {
      fs.listStatus(appPath).toSeq.filter(_.isFile).flatMap { status =>
        status.getPath.getName match {
          case "profile-driver.jfr" => Some(SparkContext.DRIVER_IDENTIFIER -> status.getPath)
          case executorFile(id) => Some(id -> status.getPath)
          case _ => None
        }
      }.sortBy(_._1)
    } else {
      Seq.empty
    }
  }

  private def flamegraph(
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent]): FlamegraphNode = {
    val fs = appPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    val root = FlamegraphNode("root", maxNodes, maxDepth)
    val tasksByExecutor = tasks.groupBy(_.executorId)
    val selected = recordings(fs).filter { case (executorId, _) =>
      selection match {
        case filter: ExecutorProfilerFilter => filter.executor.forall(_ == executorId)
        case _: TaskProfilerFilter => tasksByExecutor.contains(executorId)
      }
    }
    val parallel = parseThreads > 1 && selected.size > 1
    ProfilerStore.foreachRecording(selected, parseThreads) { case (executorId, path) =>
      // Each worker retains at most one local tree. Sequential requests use the result directly.
      val localRoot = if (parallel) FlamegraphNode("root", maxNodes, maxDepth) else root
      try {
        convertRecording(fs, path, executorId, selection,
          tasksByExecutor.getOrElse(executorId, Nil), localRoot)
      } catch {
        case NonFatal(e) if !Thread.currentThread().isInterrupted =>
          logWarning(s"Failed to parse $path for executor $executorId", e)
      }
      // Preserve partial data from a failed recording, as in the sequential path.
      if (parallel) root.merge(localRoot)
    }
    root
  }

  protected def convertRecording(
      fs: FileSystem,
      path: Path,
      executorId: String,
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent],
      root: FlamegraphNode): Unit = {
    JfrConvertor.convert(fs, path, executorId, selection, tasks, root)
  }
}

private[ui] object ProfilerStore {
  /** Submit only one task per worker, regardless of the number of recordings. */
  def foreachRecording[T](recordings: Seq[T], threads: Int)(parse: T => Unit): Unit = {
    val workers = math.min(threads, recordings.size)
    if (workers <= 1) {
      recordings.foreach(parse)
      return
    }

    val files = recordings.toIndexedSeq
    val next = new AtomicInteger()
    val pool = ThreadUtils.newDaemonFixedThreadPool(workers, "profiler-jfr-parser")
    try {
      val futures = (0 until workers).map { _ =>
        pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            var index = next.getAndIncrement()
            while (index < files.size && !Thread.currentThread().isInterrupted) {
              parse(files(index))
              index = next.getAndIncrement()
            }
          }
        })
      }
      futures.foreach(_.get())
    } finally {
      pool.shutdownNow()
      // Do not release the store's request lock while old parsers still retain its tree.
      // Await even on cancellation, then restore any interrupt received during cleanup.
      var interrupted = Thread.interrupted()
      var terminated = false
      while (!terminated) {
        try {
          terminated = pool.awaitTermination(1, TimeUnit.SECONDS)
        } catch {
          case _: InterruptedException => interrupted = true
        }
      }
      if (interrupted) Thread.currentThread().interrupt()
    }
  }
}
