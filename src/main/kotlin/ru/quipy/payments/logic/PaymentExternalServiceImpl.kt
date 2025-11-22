package ru.quipy.payments.logic

import io.netty.handler.timeout.ReadTimeoutException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeoutException

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsCollector: MetricsCollector,
    private val webClient: WebClient
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val parallelRequests = properties.parallelRequests

    private val ongoingWindow = OngoingWindow(parallelRequests, true)
    private val dbScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            ongoingWindow.acquire()
            var attempts = 0
            while (attempts < 5) {
                attempts++
                val body = webClient.post()
                    .uri(url)
                    .bodyValue("")
                    .retrieve()
                    .onStatus({ status -> status.isError }) { response ->
                        response.createException()
                    }
                    .bodyToMono(ExternalSysResponse::class.java)
                    .awaitSingle()

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                if (body.result) {
                    metricsCollector.successfulRequestInc(accountName)
                }
                else {
                    metricsCollector.failedRequestExternalInc(accountName)
                }

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }

                if (!body.result && deadline - now() > requestAverageProcessingTime) {
                    metricsCollector.incRetryCount(accountName)
                }
            }
            logger.warn("[$accountName] Payment failed after $attempts attempts for txId: $transactionId, payment: $paymentId")
        } catch (e: Exception) {
            metricsCollector.failedRequestExternalInc(accountName)

            when (e) {
                is TimeoutException, is SocketTimeoutException, is ReadTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()