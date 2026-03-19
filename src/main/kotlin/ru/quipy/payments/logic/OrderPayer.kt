package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import org.springframework.stereotype.Service
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
    private val paymentService: PaymentService,
    private val dbScope: CoroutineScope
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val paymentExecutor: ThreadPoolExecutor

    init {
        val queue = LinkedBlockingQueue<Runnable>(8_000)
        metricsCollector.requestsQueueSizeRegister(queue);

        paymentExecutor = ThreadPoolExecutor(
            16,
            16,
            0L,
            TimeUnit.MILLISECONDS,
            queue,
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            dbScope.launch {
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                while (true) {
                    try {
                        val createdEvent = paymentESService.create {
                            it.create(
                                paymentId,
                                orderId,
                                amount
                            )
                        }

                        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }

    fun getMaxRateLimit() = paymentService.getMaxRateLimit()
}