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

import scala.util.control.NonFatal

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.json4s.JsonDSL._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.internal.Logging

private[profiler] class ProfilerServlet(
    store: ProfilerStore,
    appStore: ProfilerAppStore) extends HttpServlet with Logging {

  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setContentType("application/json;charset=utf-8")
    response.setHeader("Cache-Control", "no-store")
    val result = try {
      request.getPathInfo match {
        case endpoint @ ("/filters" | "/flamegraph") =>
          val filter = ProfilerFilter.fromRequest(request)
          if (endpoint == "/filters") {
            filters(filter)
          } else {
            flamegraph(filter)
          }
        case _ =>
          response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          compact(render(("error" -> "Unknown profiler endpoint"): JValue))
      }
    } catch {
      case e: IllegalArgumentException =>
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        compact(render(("error" -> e.getMessage): JValue))
      case NonFatal(e) =>
        logWarning("Unable to load profiler data", e)
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        compact(render(("error" -> ("Unable to load profiler data. " +
          "Check the History Server logs.")): JValue))
    }
    response.getWriter.print(result)
  }

  override def doTrace(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
  }

  private def filters(selection: ProfilerFilter): String = {
    val options: JValue = selection match {
      case filter: ExecutorProfilerFilter =>
        appStore.validateExecutor(filter)
        val threads = appStore.taskThreadEvents(filter.executor)
          .map(e => (e.executorId, e.javaThreadId)).distinct.sortBy(identity)
        ("executors" -> appStore.listExecutors()) ~
          ("threads" -> threads.map { case (executorId, javaThreadId) =>
            ("id" -> s"$executorId:$javaThreadId") ~
              ("executorId" -> executorId) ~ ("javaThreadId" -> javaThreadId.toString)
          })
      case filter: TaskProfilerFilter =>
        val stages = appStore.listStages(filter)
        val stage = filter.stage.map { id =>
          stages.find(s => s"${s.stageId}:${s.attemptId}" == id)
            .getOrElse(throw new IllegalArgumentException("Unknown stage attempt"))
        }
        // Loading tasks only after a stage is chosen keeps initial filter requests small.
        val tasks = stage.toSeq.flatMap(s => appStore.listTasks(s.stageId, s.attemptId))
        ("sqlExecutions" -> appStore.listExecutions().map(_.toString)) ~
          ("jobs" -> appStore.listJobs(filter.sqlExecution).map(_.jobId.toString)) ~
          ("stages" -> stages.map { s =>
            ("id" -> s"${s.stageId}:${s.attemptId}") ~
              ("stageId" -> s.stageId) ~ ("attempt" -> s.attemptId)
          }) ~
          ("tasks" -> tasks.map { t =>
            ("id" -> t.taskId.toString) ~ ("taskId" -> t.taskId.toString) ~
              ("attempt" -> t.attempt) ~ ("partitionId" -> t.partitionId)
          }) ~
          ("hasTaskMappings" -> appStore.hasTaskThreadEvents)
    }
    compact(render(options))
  }

  private def flamegraph(selection: ProfilerFilter): String = {
    val events = selection match {
      case filter: ExecutorProfilerFilter =>
        appStore.validateExecutor(filter)
        Nil
      case filter: TaskProfilerFilter => appStore.selectedTaskEvents(filter)
    }
    store.flamegraph(selection, events).toJsonString
  }
}
