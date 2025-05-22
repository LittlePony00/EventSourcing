package com.example.infrastructure.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object KafkaConfig {
    const val ACCOUNT_EVENTS_TOPIC = "account-events"
    private const val BOOTSTRAP_SERVERS = "localhost:29092"
    private const val CONSUMER_GROUP = "account-projector"

    fun producerConfig() = Properties().apply {
        this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        this[ProducerConfig.ACKS_CONFIG] = "all"
    }

    fun consumerConfig() = Properties().apply {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        this[ConsumerConfig.GROUP_ID_CONFIG] = CONSUMER_GROUP
        this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
    }
} 