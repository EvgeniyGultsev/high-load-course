package ru.quipy.payments.logic

import io.netty.handler.timeout.ReadTimeoutException
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.PrematureCloseException
import reactor.util.retry.Retry
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingDeque
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
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val ongoingWindow = OngoingWindow(parallelRequests, true)

    private val responsesListSize = 1000
    private val responses = LinkedBlockingDeque<Long>(responsesListSize)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            ongoingWindow.acquire()
            var retryable = true
            var attempts = 0
            while (retryable && attempts < 5) {
                //rateLimiter.tickSuspend()
                retryable = false
                attempts++
                val timeout = buildTimeout(deadline, 0.95)
                val startTime = System.currentTimeMillis()
                try {
                    val body = webClient.post()
                        .uri(url)
                        .bodyValue("")
                        .retrieve()
                        .onStatus({ status -> status.isError }) { response ->
                            response.createException()
                        }
                        .bodyToMono(ExternalSysResponse::class.java)
                        .timeout(Duration.ofMillis(timeout))
                        .awaitSingle()

                    val executionTime = System.currentTimeMillis() - startTime
                    addResponseTime(executionTime)

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    if (body.result) {
                        metricsCollector.successfulRequestInc(accountName)
                    }
                    else {
                        metricsCollector.failedRequestExternalInc(accountName)
                    }

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (!body.result && deadline - now() > requestAverageProcessingTime) {
                        retryable = true
                        metricsCollector.incRetryCount(accountName)
                    }
                } catch (e: Exception) {
                    val executionTime = System.currentTimeMillis() - startTime
                    addResponseTime(executionTime)
                    
                    if (deadline - now() > requestAverageProcessingTime && attempts < 5) {
                        retryable = true
                        metricsCollector.incRetryCount(accountName)
                        delay(50)
                    }
                    else{
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            metricsCollector.failedRequestExternalInc(accountName)

            when (e) {
                is TimeoutException, is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    fun addResponseTime(executionTime: Long){
        if (responses.size >= responsesListSize - 1) responses.pollFirst()
        responses.offerLast(executionTime)
    }

    fun buildTimeout(deadline: Long, quantilePercent: Double): Long {
        val remainingTime = deadline - now()
        val minTimeout = requestAverageProcessingTime
        val maxTimeout = if (remainingTime > minTimeout) remainingTime else minTimeout
        
        val timeout = countQuantileTime(quantilePercent).coerceIn(minTimeout, maxTimeout)
        return (timeout * 1.5).toLong()
    }

    fun countQuantileTime(quantilePercent: Double): Long {
        if (quantilePercent <= 0 || quantilePercent >= 1){
            return Long.MAX_VALUE
        }

        val copy = responses.toList()
        if (copy.isEmpty()){
            return (requestAverageProcessingTime * quantilePercent).toLong()
        }

        val index = ((copy.size - 1) * quantilePercent).toInt().coerceIn(0, copy.size - 1)
        val quantileTime = copy.sorted()[index]
        metricsCollector.recordMaxRequestDuration(quantileTime, accountName)

        return quantileTime
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()