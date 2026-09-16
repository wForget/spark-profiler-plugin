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

import scala.collection.mutable.HashMap

import org.apache.commons.text.StringEscapeUtils

private[profiler] case class FlamegraphNode(name: String) {
  private val children = new HashMap[String, FlamegraphNode]()
  private var value: Long = 0L

  /** Adds a root-to-leaf stack without expanding aggregated samples into individual stacks. */
  def addStack(frames: Iterable[String], samples: Long): Unit = {
    require(samples >= 0, "Sample count must be non-negative")
    if (samples > 0) {
      value += samples
      var current = this
      frames.foreach { frame =>
        val name = StringEscapeUtils.escapeJson(frame)
        current = current.children.getOrElseUpdate(name, FlamegraphNode(name))
        current.value += samples
      }
    }
  }

  def toJsonString: String = {
    // scalastyle:off line.size.limit
    s"""{"name":"$name","value":$value,"children":[${children.map(_._2.toJsonString).mkString(",")}]}"""
    // scalastyle:on line.size.limit
  }
}
