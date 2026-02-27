package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsCollector: MetricsCollector,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val MAX_ATTEMPTS = 5
    private val TIMEOUT = Duration.ofSeconds(30)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests, false)

    private val httpThreadPoolSize = maxOf(100, parallelRequests / 10)
    private val httpExecutor = ThreadPoolExecutor(
        httpThreadPoolSize,
        httpThreadPoolSize,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(1000L))
        .version(HttpClient.Version.HTTP_2)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        val startedAt = now()
        dbScope.launch {
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            startedAt,
                            Duration.ofMillis(startedAt - paymentStartedAt)
                        )
                    }
                    break
                } catch (_: java.lang.IllegalArgumentException) {
                    delay(10)
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, 0)

    }

    private fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID, attempt: Long) {
        if (now() + requestAverageProcessingTime > deadline || attempt >= MAX_ATTEMPTS) {
            metricsCollector.failedRequestExternalInc(accountName)
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Deadline exceeded or max attempts reached")
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            metricsCollector.failedRequestInc(accountName)
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Rate limit exceed")
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = ongoingWindow.acquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            metricsCollector.failedRequestInc(accountName)
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Semaphore timeout")
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val request = HttpRequest
            .newBuilder()
            .timeout(TIMEOUT)
            .header("deadline", "$deadline")
            .header("timeout", "$TIMEOUT")
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            val currentTime = now()
            val result = body.result
            val message = body.message
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(result, currentTime, transactionId, reason = message)
                        }
                        break
                    } catch (_: java.lang.IllegalArgumentException) {
                        delay(10)
                    }
                }
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
                    metricsCollector.failedRequestExternalInc(accountName)
                    val currentTime = now()
                    dbScope.launch {
                        while (true) {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, currentTime, transactionId, reason = "Request timeout.")
                                }
                                break
                            } catch (_: java.lang.IllegalArgumentException) {
                                delay(10)
                            }
                        }
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    metricsCollector.failedRequestExternalInc(accountName)
                    val currentTime = now()
                    dbScope.launch {
                        while (true) {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, currentTime, transactionId, reason = ex.message)
                                }
                                break
                            } catch (_: java.lang.IllegalArgumentException) {
                                delay(10)
                            }
                        }
                    }
                }
            }

            metricsCollector.incRetryCount(accountName)
            ongoingWindow.release()

            performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt + 1)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()