/*
 * Copyright (C) 2018 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okio

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith

class PipeKotlinTest {
  @JvmField @Rule val timeout = org.junit.rules.Timeout(5, TimeUnit.SECONDS)

  private val executorService = Executors.newScheduledThreadPool(2)

  @After @Throws(Exception::class)
  fun tearDown() {
    executorService.shutdown()
  }

  @Test fun test() {
    val pipe = Pipe(6)
    pipe.sink.write(Buffer().writeUtf8("abc"), 3L)

    val readBuffer = Buffer()
    assertEquals(3L, pipe.source.read(readBuffer, 6L))
    assertEquals("abc", readBuffer.readUtf8())

    pipe.sink.close()
    assertEquals(-1L, pipe.source.read(readBuffer, 6L))

    pipe.source.close()
  }

  @Test
  fun fold() {
    val pipe = Pipe(128)

    val pipeSink = pipe.sink.buffer()
    pipeSink.writeUtf8("hello")
    pipeSink.emit()

    val pipeSource = pipe.source.buffer()
    assertEquals("hello", pipeSource.readUtf8(5))

    val actualSinkBuffer = Buffer()
    var actualSinkClosed = false
    val actualSink = object : ForwardingSink(actualSinkBuffer) {
      override fun close() {
        actualSinkClosed = true
        super.close()
      }
    }
    pipe.fold(actualSink)

    pipeSink.writeUtf8("world")
    pipeSink.emit()
    assertEquals("world", actualSinkBuffer.readUtf8(5))

    assertFailsWith<IllegalStateException> {
      pipeSource.readUtf8()
    }

    pipeSink.close()
    assertTrue(actualSinkClosed)
  }

  @Test
  fun foldWritesPipeContentsToSink() {
    val pipe = Pipe(128)

    val pipeSink = pipe.sink.buffer()
    pipeSink.writeUtf8("hello")
    pipeSink.emit()

    val foldSink = Buffer()
    pipe.fold(foldSink)

    assertEquals("hello", foldSink.readUtf8(5))
  }

  @Test
  fun foldUnblocksBlockedWrite() {
    val pipe = Pipe(4)
    val foldSink = Buffer()

    val latch = CountDownLatch(1)
    executorService.schedule({
      pipe.fold(foldSink)
      latch.countDown()
    }, 500, TimeUnit.MILLISECONDS)

    val sink = pipe.sink.buffer()
    sink.writeUtf8("abcdefgh") // Blocks writing 8 bytes to a 4 byte pipe.
    sink.close()

    latch.await()
    assertEquals("abcdefgh", foldSink.readUtf8())
  }

  @Test
  fun accessSinkAfterFold() {
    val pipe = Pipe(100L)
    pipe.fold(Buffer())
    assertFailsWith<IllegalStateException> {
      pipe.source.read(Buffer(), 1L)
    }
  }

  @Test
  fun honorsPipeSinkTimeoutOnWritingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    underlying.timeout.timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(biggerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingTimeoutOnWritingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    underlying.timeout.timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(smallerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkTimeoutOnFlushingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    underlying.timeout.timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(biggerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingTimeoutOnFlushingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    underlying.timeout.timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(smallerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkTimeoutOnClosingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    underlying.timeout.timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(biggerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingTimeoutOnClosingWhenItIsSmaller() {
    val smallerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    underlying.timeout.timeout(smallerTimeoutNanos, TimeUnit.NANOSECONDS)
    pipe.sink.timeout().timeout(biggerTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(smallerTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkTimeoutOnWritingWhenUnderlyingSinkTimeoutIsZero() {
    val pipeSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    pipe.sink.timeout().timeout(pipeSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(pipeSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(0L, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingSinkTimeoutOnWritingWhenPipeSinkTimeoutIsZero() {
    val underlyingSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    underlying.timeout().timeout(underlyingSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(underlyingSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingSinkTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkTimeoutOnFlushingWhenUnderlyingSinkTimeoutIsZero() {
    val pipeSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    pipe.sink.timeout().timeout(pipeSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(pipeSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(0L, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingSinkTimeoutOnFlushingWhenPipeSinkTimeoutIsZero() {
    val underlyingSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    underlying.timeout().timeout(underlyingSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(underlyingSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingSinkTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkTimeoutOnClosingWhenUnderlyingSinkTimeoutIsZero() {
    val pipeSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    pipe.sink.timeout().timeout(pipeSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(pipeSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(0L, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsUnderlyingSinkTimeoutOnClosingWhenPipeSinkTimeoutIsZero() {
    val underlyingSinkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    underlying.timeout().timeout(underlyingSinkTimeoutNanos, TimeUnit.NANOSECONDS)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(underlyingSinkTimeoutNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingSinkTimeoutNanos, underlying.timeout().timeoutNanos())
  }

  @Test
  fun honorsPipeSinkDeadlineOnWritingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    val underlyingOriginalDeadline = System.nanoTime() + biggerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + smallerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsPipeSinkDeadlineOnWritingWhenUnderlyingSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    underlying.timeout.clearDeadline()
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + deadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertFalse(underlying.timeout().hasDeadline())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnWritingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    val underlyingOriginalDeadline = System.nanoTime() + smallerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + biggerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnWritingWhenPipeSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutWritingSink()

    val underlyingOriginalDeadline = System.nanoTime() + deadlineNanos
    underlying.timeout().deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().clearDeadline()

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.write(Buffer().writeUtf8("abc"), 3)
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsPipeSinkDeadlineOnFlushingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    val underlyingOriginalDeadline = System.nanoTime() + biggerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + smallerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsPipeSinkDeadlineOnFlushingWhenUnderlyingSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    underlying.timeout.clearDeadline()
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + deadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertFalse(underlying.timeout().hasDeadline())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnFlushingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    val underlyingOriginalDeadline = System.nanoTime() + smallerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + biggerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnFlushingWhenPipeSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutFlushingSink()

    val underlyingOriginalDeadline = System.nanoTime() + deadlineNanos
    underlying.timeout().deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().clearDeadline()

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.flush()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsPipeSinkDeadlineOnClosingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    val underlyingOriginalDeadline = System.nanoTime() + biggerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + smallerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsPipeSinkDeadlineOnClosingWhenUnderlyingSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    underlying.timeout.clearDeadline()
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + deadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertFalse(underlying.timeout().hasDeadline())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnClosingWhenItIsSmaller() {
    val smallerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)
    val biggerDeadlineNanos = TimeUnit.MILLISECONDS.toNanos(1500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    val underlyingOriginalDeadline = System.nanoTime() + smallerDeadlineNanos
    underlying.timeout.deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().deadlineNanoTime(System.nanoTime() + biggerDeadlineNanos)

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(smallerDeadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  @Test
  fun honorsUnderlyingSinkDeadlineOnClosingWhenPipeSinkHasNoDeadline() {
    val deadlineNanos = TimeUnit.MILLISECONDS.toNanos(500L)

    val pipe = Pipe(4)
    val underlying = TimeoutClosingSink()

    val underlyingOriginalDeadline = System.nanoTime() + deadlineNanos
    underlying.timeout().deadlineNanoTime(underlyingOriginalDeadline)
    pipe.sink.timeout().clearDeadline()

    pipe.fold(underlying)

    val start = System.currentTimeMillis()
    pipe.sink.close()
    val elapsed = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - start)

    assertEquals(deadlineNanos.toDouble(), elapsed.toDouble(),
      TimeUnit.MILLISECONDS.toNanos(100).toDouble())
    assertEquals(underlyingOriginalDeadline, underlying.timeout().deadlineNanoTime())
  }

  /** Writes on this sink never complete. They can only time out. */
  class TimeoutWritingSink : Sink {
    val timeout = object : AsyncTimeout() {
      override fun timedOut() {
        synchronized(this@TimeoutWritingSink) {
          (this@TimeoutWritingSink as Object).notifyAll()
        }
      }
    }

    override fun write(source: Buffer, byteCount: Long) {
      timeout.enter()
      try {
        synchronized(this) {
          (this as Object).wait()
        }
      } finally {
        timeout.exit()
      }
    }

    override fun flush() = Unit

    override fun close() = Unit

    override fun timeout() = timeout
  }

  /** Flushes on this sink never complete. They can only time out. */
  class TimeoutFlushingSink : Sink {
    val timeout = object : AsyncTimeout() {
      override fun timedOut() {
        synchronized(this@TimeoutFlushingSink) {
          (this@TimeoutFlushingSink as Object).notifyAll()
        }
      }
    }

    override fun write(source: Buffer, byteCount: Long) = Unit

    override fun flush() {
      timeout.enter()
      try {
        synchronized(this) {
          (this as Object).wait()
        }
      } finally {
        timeout.exit()
      }
    }

    override fun close() = Unit

    override fun timeout() = timeout
  }

  /** Closes on this sink never complete. They can only time out. */
  class TimeoutClosingSink : Sink {
    val timeout = object : AsyncTimeout() {
      override fun timedOut() {
        synchronized(this@TimeoutClosingSink) {
          (this@TimeoutClosingSink as Object).notifyAll()
        }
      }
    }

    override fun write(source: Buffer, byteCount: Long) = Unit

    override fun flush() = Unit

    override fun close() {
      timeout.enter()
      try {
        synchronized(this) {
          (this as Object).wait()
        }
      } finally {
        timeout.exit()
      }
    }

    override fun timeout() = timeout
  }
}