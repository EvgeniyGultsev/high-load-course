package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import ru.quipy.apigateway.TooManyRequestsException
import ru.quipy.common.utils.SlidingWindowRateLimiter

@Configuration
class WebClientConfig {
    fun rateLimitingFilter(rateLimiter: SlidingWindowRateLimiter): ExchangeFilterFunction =
        ExchangeFilterFunction.ofRequestProcessor { req: ClientRequest ->
            if (rateLimiter.tick()) {
                Mono.just(req)
            } else {
                Mono.error(TooManyRequestsException())
            }
        }


    @Bean
    fun webClient(): WebClient {
        val connectionProvider = ConnectionProvider
            .builder("connection_provider")
            .maxConnections(10_000)
            .build()

        val httpClient = HttpClient
            .create(connectionProvider)
            //.protocol(HttpProtocol.H2C)

        return WebClient
            .builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            // .filter(rateLimitingFilter(SlidingWindowRateLimiter(1100)))
            .build()
    }
}