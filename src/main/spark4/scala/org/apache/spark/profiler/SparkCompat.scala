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

import org.apache.spark.internal.LogKeys.{EXECUTOR_ID, PATH}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[profiler] object SparkCompat {
  type HttpServlet = jakarta.servlet.http.HttpServlet
  type HttpServletRequest = jakarta.servlet.http.HttpServletRequest
  type HttpServletResponse = jakarta.servlet.http.HttpServletResponse

  val chartAssets = "/static"

  def nameForAppAndAttempt(appId: String, attemptId: Option[String]): String =
    Utils.nameForAppAndAttempt(appId, attemptId)
}

private[profiler] trait ProfilerLogging extends Logging {
  protected def logExecutorSelected(executorId: String): Unit =
    logInfo(log"Executor id ${MDC(EXECUTOR_ID, executorId)} selected for profiling")

  protected def logCopyTo(path: String): Unit =
    logInfo(log"Copying profiling file to ${MDC(PATH, path)}")
}
