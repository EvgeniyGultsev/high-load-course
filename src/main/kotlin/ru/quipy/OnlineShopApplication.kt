package ru.quipy

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        // Глобальный корутин scope для асинхронных задач приложения
        val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    }
}

fun main(args: Array<String>) {
    runApplication<OnlineShopApplication>(*args)
}
