package com.example.service

import com.example.domain.Account
import com.example.domain.AccountEvent
import com.example.infrastructure.db.AccountRepository
import com.example.infrastructure.kafka.KafkaConfig
import kotlinx.coroutines.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class AccountProjectorService(
    private val repository: AccountRepository,
    private val consumer: KafkaConsumer<String, String> = KafkaConsumer(KafkaConfig.consumerConfig())
) : CoroutineScope {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val json = Json { 
        ignoreUnknownKeys = true 
        encodeDefaults = true
    }
    private val job = Job()
    override val coroutineContext: CoroutineContext = job + Dispatchers.IO

    init {
        consumer.subscribe(listOf(KafkaConfig.ACCOUNT_EVENTS_TOPIC))
        logger.info("Subscribed to topic: ${KafkaConfig.ACCOUNT_EVENTS_TOPIC}")
    }

    fun start() {
        launch {
            try {
                logger.info("Starting AccountProjectorService")
                while (isActive) {
                    processEvents()
                }
            } catch (e: Exception) {
                logger.error("Error processing events", e)
            } finally {
                consumer.close()
            }
        }
    }

    private fun processEvents() {
        logger.debug("Polling for events...")
        val records = consumer.poll(Duration.ofMillis(1000))
        logger.info("Received ${records.count()} records")
        
        val eventsByAccount = records.map { record ->
            logger.debug("Processing record: ${record.value()}")
            json.decodeFromString<AccountEvent>(record.value())
        }.groupBy { it.accountId }

        eventsByAccount.forEach { (accountId, events) ->
            try {
                processAccountEvents(accountId, events)
            } catch (e: Exception) {
                logger.error("Error processing events for account $accountId", e)
            }
        }

        consumer.commitSync()
    }

    private fun processAccountEvents(accountId: java.util.UUID, events: List<AccountEvent>) {
        val initialAccount = repository.findById(accountId)
        logger.info("Current account state for $accountId: $initialAccount")

        events.sortedBy { it.version }
            .fold(initialAccount ?: Account.create(accountId)) { acc, event ->
                if (event.version > acc.version) {
                    val updatedAccount = acc.apply(event)
                    repository.save(updatedAccount)
                    logger.info("Applied event ${event::class.simpleName} to account $accountId, new state: $updatedAccount")
                    updatedAccount
                } else {
                    logger.info("Skipping event ${event::class.simpleName} for account $accountId as version ${event.version} <= ${acc.version}")
                    acc
                }
            }
    }

    fun stop() {
        job.cancel()
    }
} 