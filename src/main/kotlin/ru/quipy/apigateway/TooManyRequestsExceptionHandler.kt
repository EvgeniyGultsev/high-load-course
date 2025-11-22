package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import reactor.core.publisher.Mono

@ControllerAdvice
class TooManyRequestsExceptionHandler {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TooManyRequestsExceptionHandler::class.java)
        private const val RETRY_AFTER_IN_SECONDS = 2
    }

    @ExceptionHandler
    fun handleTooManyRequestsException(e: TooManyRequestsException): Mono<ResponseEntity<String>> {
        val headers = HttpHeaders()
        val retryAfterTimestampInMillis = (System.currentTimeMillis() + RETRY_AFTER_IN_SECONDS * 1000).toString()
        headers.add("Retry-After", retryAfterTimestampInMillis)
        return Mono.just(
            ResponseEntity("Too many requests", headers, HttpStatus.TOO_MANY_REQUESTS)
        )
    }
}