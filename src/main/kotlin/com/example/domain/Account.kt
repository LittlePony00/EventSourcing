package com.example.domain

import com.example.infrastructure.serialization.BigDecimalSerializer
import com.example.infrastructure.serialization.InstantSerializer
import com.example.infrastructure.serialization.UUIDSerializer
import kotlinx.serialization.Serializable
import java.math.BigDecimal
import java.time.Instant
import java.util.*

data class Account(
    val id: UUID,
    val balance: BigDecimal,
    val status: AccountStatus,
    val version: Long
) {
    companion object {
        fun create(id: UUID): Account = Account(
            id = id,
            balance = BigDecimal.ZERO,
            status = AccountStatus.ACTIVE,
            version = 0
        )
    }

    fun apply(event: AccountEvent): Account = when (event) {
        is AccountEvent.AccountCreated -> copy(
            id = event.accountId,
            status = AccountStatus.ACTIVE,
            version = event.version
        )
        is AccountEvent.MoneyDeposited -> copy(
            balance = balance.add(event.amount),
            version = event.version
        )
        is AccountEvent.MoneyWithdrawn -> copy(
            balance = balance.subtract(event.amount),
            version = event.version
        )
    }
}

enum class AccountStatus {
    ACTIVE,
    CLOSED
}

@Serializable
sealed class AccountEvent {
    @Serializable(with = UUIDSerializer::class)
    abstract val accountId: UUID
    abstract val version: Long
    @Serializable(with = InstantSerializer::class)
    abstract val timestamp: Instant

    @Serializable
    data class AccountCreated(
        @Serializable(with = UUIDSerializer::class)
        override val accountId: UUID,
        override val version: Long,
        @Serializable(with = InstantSerializer::class)
        override val timestamp: Instant = Instant.now()
    ) : AccountEvent()

    @Serializable
    data class MoneyDeposited(
        @Serializable(with = UUIDSerializer::class)
        override val accountId: UUID,
        override val version: Long,
        @Serializable(with = BigDecimalSerializer::class)
        val amount: BigDecimal,
        @Serializable(with = InstantSerializer::class)
        override val timestamp: Instant = Instant.now()
    ) : AccountEvent()

    @Serializable
    data class MoneyWithdrawn(
        @Serializable(with = UUIDSerializer::class)
        override val accountId: UUID,
        override val version: Long,
        @Serializable(with = BigDecimalSerializer::class)
        val amount: BigDecimal,
        @Serializable(with = InstantSerializer::class)
        override val timestamp: Instant = Instant.now()
    ) : AccountEvent()
} 