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

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, NonWritableChannelException, SeekableByteChannel}

import org.apache.hadoop.fs.{FSDataInputStream, StreamCapabilities}

/**
 * A read-only channel over a fixed-size file snapshot. Obtain `fileSize` from the file status
 * when opening the stream; later file growth is not exposed through this channel.
 *
 * The channel takes exclusive ownership of `input`, including its current position and closing
 * it. The caller must not read, seek or close the stream separately while the channel is in use.
 */
private[profiler] final class FSDataInputStreamChannel(
    input: FSDataInputStream,
    fileSize: Long) extends SeekableByteChannel {

  require(fileSize >= 0, "File size must be non-negative")

  @volatile private var open = true
  private var currentPosition = input.getPos
  private val supportsByteBuffer = input.hasCapability(StreamCapabilities.READBYTEBUFFER)
  private lazy val transferBuffer = new Array[Byte](64 * 1024)

  override def read(dst: ByteBuffer): Int = synchronized {
    ensureOpen()
    require(!dst.isReadOnly, "Destination buffer must be writable")
    if (!dst.hasRemaining) return 0
    if (currentPosition >= fileSize) return -1

    val length = math.min(dst.remaining().toLong, fileSize - currentPosition).toInt
    val count = if (dst.hasArray) {
      val n = input.read(dst.array(), dst.arrayOffset() + dst.position(), length)
      if (n > 0) dst.position(dst.position() + n)
      n
    } else if (supportsByteBuffer) {
      // Bound reads to the snapshot size without changing the caller's buffer limit.
      val buffer = dst.duplicate()
      buffer.limit(buffer.position() + length)
      val n = input.read(buffer)
      if (n > 0) dst.position(dst.position() + n)
      n
    } else {
      // Some Hadoop file systems only implement byte-array reads.
      val n = input.read(transferBuffer, 0, math.min(length, transferBuffer.length))
      if (n > 0) dst.put(transferBuffer, 0, n)
      n
    }
    if (count > 0) currentPosition += count
    count
  }

  override def position(): Long = synchronized {
    ensureOpen()
    currentPosition
  }

  override def position(newPosition: Long): FSDataInputStreamChannel = synchronized {
    ensureOpen()
    require(newPosition >= 0, "Position must be non-negative")
    // Hadoop streams may reject seeks past EOF, which SeekableByteChannel must allow.
    if (newPosition <= fileSize) input.seek(newPosition)
    currentPosition = newPosition
    this
  }

  override def size(): Long = synchronized {
    ensureOpen()
    fileSize
  }

  override def write(src: ByteBuffer): Int = synchronized {
    ensureOpen()
    throw new NonWritableChannelException()
  }

  override def truncate(size: Long): FSDataInputStreamChannel = synchronized {
    ensureOpen()
    require(size >= 0, "Size must be non-negative")
    throw new NonWritableChannelException()
  }

  override def isOpen: Boolean = open

  override def close(): Unit = synchronized {
    if (open) {
      open = false
      input.close()
    }
  }

  private def ensureOpen(): Unit = {
    if (!open) throw new ClosedChannelException()
  }
}
