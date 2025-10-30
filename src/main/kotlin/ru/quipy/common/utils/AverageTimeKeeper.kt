package ru.quipy.common.utils

import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.sqrt

@Component
@Scope("singleton")
class AverageTimeKeeper {
    private val average = AtomicLong(0)
    private val allTimes = ArrayDeque<Long>()
    private val maxSize = 30
    private val theta = 0.1;

    fun record(processingTimeMillis: Long) {
        while (true) {
            val current = average.get()
            val newCur: Double = if (current == 0L)
                processingTimeMillis.toDouble()
            else {
                processingTimeMillis.toDouble() * theta + current.toDouble() * (1.0 - theta)
            }

            if (average.compareAndSet(current, newCur.toLong())) {
                break
            }
        }
    }

    fun getAverage(): Long {
        return average.get()
    }

    fun recordExternalServiceProcessingTime(processingTimeMillis: Long) {
        allTimes.addLast(processingTimeMillis)
        if (allTimes.size > maxSize) {
            allTimes.removeFirst()
        }
    }

    fun calculateStandardDeviation(): Double {
        val times = allTimes.toList()

        if (times.isEmpty()) return 0.0
        if (times.size == 1) return 0.0

        val mean = times.average()
        val variance = times.map { time ->
            (time - mean) * (time - mean)
        }.average()

        return sqrt(variance)
    }
}