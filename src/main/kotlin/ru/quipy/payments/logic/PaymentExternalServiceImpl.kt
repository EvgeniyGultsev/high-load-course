package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsCollector: MetricsCollector
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val MAX_ATTEMPTS = 5

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val ongoingWindow = OngoingWindow(parallelRequests, true)

    private val client = HttpClient
        .newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    // private val responsesListSize = 1000
    // private val responses = LinkedBlockingDeque<Long>(responsesListSize)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, 0)
    }

    private fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID, attempt: Long) {
        if (now() + requestAverageProcessingTime > deadline || attempt >= MAX_ATTEMPTS) {
            metricsCollector.failedRequestExternalInc(accountName)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded or max attempts reached")
            }
            return
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            metricsCollector.failedRequestInc(accountName)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceed")
            }
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = ongoingWindow.acquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            metricsCollector.failedRequestInc(accountName)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
            }
            return
        }

        // val timeout = getTimeout(deadline, 0.99)
        val request = HttpRequest
            .newBuilder()
//            .timeout(Duration.ofMillis(timeout))
//            .header("deadline", "$deadline")
//            .header("timeout", "$timeout")
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        // val startTime = now()
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
            // TODO разобраться почему response time зависают
            // addResponseTime(now() - startTime)

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            if (body.result) {
                metricsCollector.successfulRequestInc(accountName)
                ongoingWindow.release()
            }
            else {
                metricsCollector.incRetryCount(accountName)
                ongoingWindow.release()

                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt + 1)
            }

        }.exceptionally { ex ->
            when (ex) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = ex.message)
                    }
                }
            }

            metricsCollector.incRetryCount(accountName)
            ongoingWindow.release()
            performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt + 1)
        }
    }

//    fun addResponseTime(executionTimeMillis: Long){
//        if (responses.size >= responsesListSize - 1) responses.pollFirst()
//        responses.offerLast(executionTimeMillis)
//    }
//
//    fun getTimeout(deadline: Long, quantilePercent: Double): Long {
//        return countQuantileTime(quantilePercent).coerceIn(requestAverageProcessingTime, deadline - now())
//    }
//
//    fun countQuantileTime(quantilePercent: Double): Long {
//        if (quantilePercent <= 0 || quantilePercent >= 1){
//            return Long.MAX_VALUE
//        }
//
//        val copy = responses.toList()
//        if (copy.count() < 10){
//            return (requestAverageProcessingTime * 1.2 * quantilePercent).toLong()
//        }
//
//        val index = ((copy.size - 1) * quantilePercent).toInt().coerceIn(0, copy.size - 1)
//        val quantileTime = copy.sorted()[index]
//        metricsCollector.recordMaxRequestDuration(quantileTime, accountName)
//
//        return quantileTime
//    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()