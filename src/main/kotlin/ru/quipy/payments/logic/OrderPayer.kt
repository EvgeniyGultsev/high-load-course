package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.AverageTimeKeeper
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val metricsCollector: MetricsCollector,
    private val paymentService: PaymentService
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    // private var avgTimeKeeper = AverageTimeKeeper()

    private val paymentExecutor: ThreadPoolExecutor

//    private val paymentTaskQueue = LinkedBlockingQueue<Payment>()
//    private val backgroundWorkers = getMaxRateLimit()

    init {
        val queue = LinkedBlockingQueue<Runnable>(8000)
        metricsCollector.requestsQueueSizeRegister(queue);

        paymentExecutor = ThreadPoolExecutor(
            200,
            1200,
            60L,
            TimeUnit.SECONDS,
            queue,
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
//        val averageProcessingTime = avgTimeKeeper.getAverage()
//
//        val maxProcessingTime = averageProcessingTime * 1
//
//        val queueProcessingTime = (paymentTaskQueue.size + backgroundWorkers) * maxProcessingTime / backgroundWorkers
//
//        if (now() + queueProcessingTime > deadline) {
//            logger.warn("Payment $paymentId for order $orderId not created (too many requests)")
//            metricsCollector.status429RequestInc()
//
//            return null
//        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }

            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }

    fun getMaxRateLimit() = paymentService.getMaxRateLimit()
}