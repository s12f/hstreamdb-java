package io.hstream.impl

import io.hstream.BufferedProducer
import io.hstream.HStreamDBClientException
import io.hstream.RecordId
import io.hstream.internal.HStreamRecord
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import kotlin.concurrent.withLock

class BufferedProducerKtImpl(
    stream: String,
    private val recordCountLimit: Int,
    private val flushIntervalMs: Long,
    private val maxBytesSize: Int,
    private val throwExceptionIfFull: Boolean
) : ProducerKtImpl(stream), BufferedProducer {
    private var lock = ReentrantLock()
    private var recordBuffer: MutableList<HStreamRecord> = ArrayList(recordCountLimit)
    private var futures: MutableList<CompletableFuture<RecordId>> = ArrayList(recordCountLimit)
    private var timerService: ScheduledFuture<*>? = null

    @Volatile
    private var closed: Boolean = false
    private var bufferedBytesSize: Int = 0

    @Volatile
    private var isFull = false

    init {
        if (flushIntervalMs > 0) {
            runTimer()
        }
    }

    private fun runTimer() {
        timerService = scheduler.scheduleAtFixedRate(
            { flush() },
            flushIntervalMs,
            flushIntervalMs,
            TimeUnit.MILLISECONDS
        )
    }

    override fun writeInternal(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        return addToBuffer(hStreamRecord)
    }

    private fun addToBuffer(hStreamRecord: HStreamRecord): CompletableFuture<RecordId> {
        // fuzzy check
        val recordFuture = CompletableFuture<RecordId>()
        if (throwExceptionIfFull && isFull) {
            recordFuture.completeExceptionally(HStreamDBClientException("buffer is full"))
            return recordFuture
        }
        lock.withLock {
            if (closed) {
                throw HStreamDBClientException("BufferedProducer is closed")
            }
            // it is impossible that buffer is full after holding the lock,
            // if buffer is full, there must exist another thread keeping the lock(flushing buffer).
            recordBuffer.add(hStreamRecord)
            futures.add(recordFuture)
            bufferedBytesSize += hStreamRecord.payload.size()
            if (isFull()) {
                isFull = true
                flush()
            }
            return recordFuture
        }
    }

    private fun isFull(): Boolean {
        return (recordBuffer.size == recordCountLimit) || maxBytesSize > 0 && bufferedBytesSize >= maxBytesSize
    }

    override fun flush() {
        lock.withLock {
            if (recordBuffer.isEmpty()) {
                return
            }
            val recordBufferCount = recordBuffer.size
            logger.info("ready to flush recordBuffer, current buffer size is [{}]", recordBufferCount)
            runBlocking(Dispatchers.IO) { groupWriteHStreamRecords() }
            logger.info("flush the record buffer successfully")
            recordBuffer.clear()
            futures.clear()
            bufferedBytesSize = 0
            isFull = false
        }
    }

    // only can be called by flush()
    private suspend fun groupWriteHStreamRecords() {
        val recordGroup =
            mutableMapOf<String, Pair<MutableList<HStreamRecord>, MutableList<CompletableFuture<RecordId>>>>()
        for (i in recordBuffer.indices) {
            val key = recordBuffer[i].header.key
            if (!recordGroup.containsKey(key)) {
                recordGroup[key] = Pair(mutableListOf(), mutableListOf())
            }
            recordGroup[key]!!.first += recordBuffer[i]
            recordGroup[key]!!.second += futures[i]
        }
        coroutineScope {
            for ((key, pair) in recordGroup) {
                launch {
                    try {
                        val ids = super.writeHStreamRecords(pair.first, key)
                        for (i in ids.indices) {
                            pair.second[i].complete(ids[i])
                        }
                    } catch (e: Throwable) {
                        pair.second.forEach { it.completeExceptionally(e) }
                    }
                }
            }
        }
    }

    override fun close() {
        if (!closed) {
            timerService?.cancel(false)
            closed = true
            flush()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BufferedProducerKtImpl::class.java)
        private val scheduler = Executors.newScheduledThreadPool(4)
    }
}
