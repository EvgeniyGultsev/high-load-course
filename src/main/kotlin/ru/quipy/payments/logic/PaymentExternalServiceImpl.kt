package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import ru.quipy.common.utils.RateLimiterException
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsCollector
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
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

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val MAX_ATTEMPTS = 3;
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter((rateLimitPerSec).toLong())

    private inline fun <reified T> rateLimited(mono: Mono<T>): Mono<T> =
        Mono.fromCallable { rateLimiter.tick() }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { permitted ->
                if (permitted) mono
                else Mono.error(RateLimiterException())
            }

    // private val ongoingWindow = OngoingWindow(parallelRequests, true)

    // private val responsesListSize = 1000
    // private val responses = LinkedBlockingDeque<Long>(responsesListSize)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        performPaymentInternalAsync(paymentId, amount, paymentStartedAt, deadline, 0)
    }

    suspend fun performPaymentInternalAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Long) {
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

            // ongoingWindow.acquire()
//            var retryable = true
//            while (retryable) {
//                //rateLimiter.tickSuspend()
//                retryable = false
                // val timeout = buildTimeout(deadline, 0.95)
                //val startTime = System.currentTimeMillis()
                // try {
                    val request = webClient
                        .post()
                        .uri(url)
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .toEntity(ExternalSysResponse::class.java)

                    val response = rateLimited(request)
                        .awaitSingle()

                    // val executionTime = System.currentTimeMillis() - startTime
                    // addResponseTime(executionTime)

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${response.body!!.result}, message: ${response.body!!.message}")
//                    if (response.body!!.result) {
//                        metricsCollector.successfulRequestInc(accountName)
//                    }
//                    else {
//                        metricsCollector.failedRequestExternalInc(accountName)
//                    }

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(response.body!!.result, now(), transactionId, reason = response.body!!.message)
                    }

//                    if (!response.body!!.result && deadline - now() > requestAverageProcessingTime) {
//                        retryable = true
//                        metricsCollector.incRetryCount(accountName)
//                    }
//                } catch (e: Exception) {
//                    val executionTime = System.currentTimeMillis() - startTime
//                    addResponseTime(executionTime)
//
//                    if (deadline - now() > requestAverageProcessingTime) {
//                        retryable = true
//                        metricsCollector.incRetryCount(accountName)
//                    }
//                    else{
//                        throw e
//                    }
//                }
//            }
        } catch (e: Exception) {
            when (e) {
                is TimeoutException, is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                    metricsCollector.failedRequestExternalInc(accountName)
                }
                is RateLimiterException -> {
                    logger.error("[$accountName] Too many requests for txId: $transactionId, payment: $paymentId")
                    if (attempt < MAX_ATTEMPTS && deadline - now() > requestAverageProcessingTime) {
                        performPaymentInternalAsync(paymentId, amount, paymentStartedAt, deadline, attempt + 1)
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    metricsCollector.failedRequestExternalInc(accountName)
                }
            }
        }
/*        finally {
            ongoingWindow.release()
        }*/
    }

//    fun addResponseTime(executionTime: Long){
//        if (responses.size >= responsesListSize - 1) responses.pollFirst()
//        responses.offerLast(executionTime)
//    }
//
//    fun buildTimeout(deadline: Long, quantilePercent: Double): Long {
//        val remainingTime = deadline - now()
//        val minTimeout = requestAverageProcessingTime
//        val maxTimeout = if (remainingTime > minTimeout) remainingTime else minTimeout
//
//        val timeout = countQuantileTime(quantilePercent).coerceIn(minTimeout, maxTimeout)
//        return (timeout * 1.5).toLong()
//    }
//
//    fun countQuantileTime(quantilePercent: Double): Long {
//        if (quantilePercent <= 0 || quantilePercent >= 1){
//            return Long.MAX_VALUE
//        }
//
//        val copy = responses.toList()
//        if (copy.isEmpty()){
//            return (requestAverageProcessingTime * quantilePercent).toLong()
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