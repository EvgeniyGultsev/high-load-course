package ru.quipy.payments.logic

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.AverageTimeKeeper
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import kotlinx.coroutines.channels.Channel

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

    private var avgTimeKeeper = AverageTimeKeeper()

    private val paymentTaskQueue = Channel<Payment>(Channel.UNLIMITED)
    private val backgroundWorkers = getMaxRateLimit()
    private val paymentScope = CoroutineScope(Dispatchers.IO)

    init {
        paymentScope.launch {
            while (isActive) {
                try {
                    val task = paymentTaskQueue.receive()
                    val taskStartedAt = now()
                    processInternal(task)
                    avgTimeKeeper.record(now() - taskStartedAt)
                } catch (e: Exception) {
                    if (e is CancellationException) {
                        break
                    }
                    logger.error("Error processing payment task", e)
                }
            }
        }
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        val averageProcessingTime = avgTimeKeeper.getAverage()

        val maxProcessingTime = averageProcessingTime * 1

        val queueProcessingTime = backgroundWorkers * maxProcessingTime / backgroundWorkers

        if (now() + queueProcessingTime > deadline) {
            logger.warn("Payment $paymentId for order $orderId not created (too many requests)")
            metricsCollector.status429RequestInc()

            return null
        }

        paymentTaskQueue.send(Payment(orderId, amount, paymentId, deadline, createdAt))
        return createdAt
    }

    private suspend fun processInternal(payment: Payment){
        val createdEvent = paymentESService.create {
            it.create(
                payment.paymentId,
                payment.orderId,
                payment.amount
            )
        }

        logger.trace("Payment ${createdEvent.paymentId} for order $payment.orderId created.")

        paymentService.submitPaymentRequest(payment.paymentId, payment.amount, payment.createdAt, payment.deadline)
    }

    private data class Payment(
        val orderId: UUID,
        val amount: Int,
        val paymentId: UUID,
        val deadline: Long,
        val createdAt: Long
    )
    fun getMaxRateLimit() = paymentService.getMaxRateLimit()
}