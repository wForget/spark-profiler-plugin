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
package org.apache.spark.profiler.jfr

import java.io.{StringWriter, Writer}

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.commons.text.StringEscapeUtils

private[profiler] final class FlamegraphNode private (
    val name: String,
    budget: FlamegraphNode.Budget) {
  private val children = new HashMap[String, FlamegraphNode]()
  private var value: Long = 0L
  private var omitted: Long = 0L

  /**
   * Adds a root-to-leaf stack without expanding aggregated samples into individual stacks.
   * The tree must be owned by the calling parser; this operation does not acquire a lock.
   */
  def addStack(frames: Iterable[String], samples: Long): Unit = {
    require(samples >= 0, "Sample count must be non-negative")
    if (samples > 0) {
      value += samples
      var current = this
      var depth = 0
      val iterator = frames.iterator
      var expanding = true
      while (expanding && iterator.hasNext) {
        if (depth >= budget.maxDepth) {
          current.omitted += samples
          expanding = false
        } else {
          val frame = iterator.next()
          val child = current.children.get(frame).orElse {
            if (budget.nodes < budget.maxNodes) {
              val node = new FlamegraphNode(frame, budget)
              current.children.put(frame, node)
              budget.nodes += 1
              Some(node)
            } else {
              None
            }
          }
          child match {
            case Some(node) =>
              current = node
              current.value += samples
              depth += 1
            case None =>
              current.omitted += samples
              expanding = false
          }
        }
      }
    }
  }

  /**
   * Copies a completed parser tree into this root while respecting this tree's budgets.
   * The source must not be modified during the merge. Concurrent merges into this root are
   * allowed, but addStack and serialization must not run concurrently with a merge.
   */
  def merge(source: FlamegraphNode): Unit = synchronized {
    require(source ne this, "Cannot merge a flame graph into itself")
    require(source.name == name, "Flame graph roots must have the same name")

    class Pending(val target: FlamegraphNode, source: FlamegraphNode, val depth: Int) {
      val remaining: Iterator[FlamegraphNode] = source.children.valuesIterator
      target.value += source.value
      target.omitted += source.omitted
    }

    // Iterative traversal avoids overflowing the JVM stack for large configured depths.
    val pending = new ArrayBuffer[Pending]()
    pending += new Pending(this, source, 0)
    while (pending.nonEmpty) {
      val parent = pending.last
      if (parent.remaining.hasNext) {
        val child = parent.remaining.next()
        val target = if (parent.depth >= budget.maxDepth) {
          None
        } else {
          parent.target.children.get(child.name).orElse {
            if (budget.nodes < budget.maxNodes) {
              val node = new FlamegraphNode(child.name, budget)
              parent.target.children.put(child.name, node)
              budget.nodes += 1
              Some(node)
            } else {
              None
            }
          }
        }
        target match {
          case Some(node) => pending += new Pending(node, child, parent.depth + 1)
          case None => parent.target.omitted += child.value
        }
      } else {
        pending.remove(pending.size - 1)
      }
    }
  }

  def toJsonString: String = {
    val writer = new StringWriter()
    writeJson(writer)
    writer.toString
  }

  /** Writes directly to the caller's writer, using O(depth) traversal state. */
  def writeJson(writer: Writer, minFraction: Double = 0.0001): Unit = {
    require(minFraction >= 0.0 && minFraction <= 1.0,
      "Minimum flame graph fraction must be in [0,1]")
    val threshold = value.toDouble * minFraction

    class Pending(node: FlamegraphNode) {
      val remaining: Iterator[FlamegraphNode] = node.children.valuesIterator
      var other: Long = node.omitted
      var first: Boolean = true

      def separator(): Unit = {
        if (!first) writer.write(',')
        first = false
      }
    }

    def start(nodeName: String, samples: Long): Unit = {
      writer.write("{\"name\":\"")
      StringEscapeUtils.ESCAPE_JSON.translate(nodeName, writer)
      writer.write("\",\"value\":")
      writer.write(samples.toString)
      writer.write(",\"children\":[")
    }

    start(name, value)
    val pending = new ArrayBuffer[Pending]()
    pending += new Pending(this)
    while (pending.nonEmpty) {
      val parent = pending.last
      if (parent.remaining.hasNext) {
        val child = parent.remaining.next()
        if (child.value.toDouble < threshold) {
          // Inclusive counts allow skipping the entire subtree without visiting its descendants.
          parent.other += child.value
        } else {
          parent.separator()
          start(child.name, child.value)
          pending += new Pending(child)
        }
      } else {
        if (parent.other > 0) {
          parent.separator()
          start("[other]", parent.other)
          writer.write("]}")
        }
        writer.write("]}")
        pending.remove(pending.size - 1)
      }
    }
  }
}

private[profiler] object FlamegraphNode {
  private class Budget(val maxNodes: Int, val maxDepth: Int) {
    var nodes: Int = 1 // Include the root; synthetic [other] leaves are emitted only at write time.
  }

  def apply(name: String, maxNodes: Int = 100000, maxDepth: Int = 256): FlamegraphNode = {
    require(maxNodes > 0, "Maximum flame graph node count must be positive")
    require(maxDepth >= 0, "Maximum flame graph depth must be non-negative")
    new FlamegraphNode(name, new Budget(maxNodes, maxDepth))
  }
}
