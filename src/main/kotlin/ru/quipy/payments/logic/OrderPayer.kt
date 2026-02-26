package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
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

    private val paymentExecutor: ThreadPoolExecutor

    // Dedicated executor for event sourcing updates to avoid blocking at high RPS
    private val esUpdateExecutor = ThreadPoolExecutor(
        50,
        100,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(10000),
        NamedThreadFactory("es-update-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    init {
        val queue = LinkedBlockingQueue<Runnable>(20000)
        metricsCollector.requestsQueueSizeRegister(queue);

        paymentExecutor = ThreadPoolExecutor(
            50,
            50,
            60L,
            TimeUnit.SECONDS,
            queue,
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    private fun createESAsync(
        createFn: () -> Event<PaymentAggregate>
    ) {
        esUpdateExecutor.submit {
            try {
                createFn()
            } catch (e: Exception) {
                logger.error("Failed to create payment in ES", e)
            }
        }
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            createESAsync {
                paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
            }

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }

    fun getMaxRateLimit() = paymentService.getMaxRateLimit()
}