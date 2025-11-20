package ru.quipy.common.utils

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.payments.logic.now
import java.time.Duration
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration = Duration.ofSeconds(1),
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)
    
    // Асинхронная очередь ожидающих корутин
    private val waitingQueue = Channel<CompletableDeferred<Unit>>(Channel.UNLIMITED)
    private val mutex = Mutex()

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    /**
     * Полностью асинхронная версия - не блокирует поток, использует каналы корутин
     */
    suspend fun tickSuspend() {
        // Быстрая проверка без блокировки
        if (tick()) {
            return
        }
        
        // Если лимит достигнут, добавляем корутину в очередь ожидания
        while (true) {
            val deferred = CompletableDeferred<Unit>()
            
            mutex.withLock {
                // Повторная проверка после получения блокировки
                if (tick()) {
                    deferred.complete(Unit)
                    return@withLock
                }
                
                // Добавляем в очередь ожидания
                waitingQueue.send(deferred)
            }
            
            // Ждем разрешения (неблокирующее ожидание)
            deferred.await()
            
            // После пробуждения снова проверяем - возможно слот уже занят другой корутиной
            if (tick()) {
                return
            }
            // Если не получилось, продолжаем ждать
        }
    }

    fun tickBlocking(timeout: Long, unit: TimeUnit): Boolean {
        if (timeout <= 0) return false
        val start = now()
        val timeoutMillis = unit.toMillis(timeout)

        while (!tick()) {
            Thread.sleep(10)

            if (now() > start + timeoutMillis) {
                return false
            }
        }

        return true
    }

    /**
     * Блокирующая версия для использования в реактивном коде (Mono)
     * Возвращает true если разрешение получено, false если нет
     */
    fun acquirePermission(): Boolean {
        return tick()
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null) {
                delay(1L)
                continue
            }
            if (head.timestamp > winStart) {
                delay(head.timestamp - winStart)
                continue
            }
            sum.addAndGet(-1)
            // Используем poll() вместо take() чтобы не блокировать
            // Если очередь пуста, это не должно произойти, но на всякий случай
            queue.poll() ?: continue
            
            // Пробуждаем ожидающие корутины после освобождения слота
            mutex.withLock {
                // Пробуждаем одну корутину из очереди
                val waiting = waitingQueue.tryReceive()
                if (waiting.isSuccess) {
                    waiting.getOrNull()?.complete(Unit)
                }
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}