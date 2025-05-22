package com.example.infrastructure.kafka

import com.example.domain.AccountEvent
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

class AccountEventProducer(
    private val producer: KafkaProducer<String, String> = KafkaProducer(KafkaConfig.producerConfig())
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val json = Json { 
        ignoreUnknownKeys = true 
        encodeDefaults = true
    }

    fun publish(event: AccountEvent) {
        try {
            val record = ProducerRecord(
                KafkaConfig.ACCOUNT_EVENTS_TOPIC,
                event.accountId.toString(),
                json.encodeToString(event)
            )

            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    logger.error("Failed to publish event: ${event::class.simpleName}", exception)
                } else {
                    logger.info(
                        "Published event: ${event::class.simpleName} to topic: ${metadata.topic()}, " +
                        "partition: ${metadata.partition()}, offset: ${metadata.offset()}"
                    )
                }
            }
        } catch (e: Exception) {
            logger.error("Error publishing event: ${event::class.simpleName}", e)
            throw e
        }
    }

    fun close() {
        producer.close()
    }
} 