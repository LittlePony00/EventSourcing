package com.example

import com.example.domain.AccountCommand
import com.example.domain.AccountEvent
import com.example.infrastructure.db.AccountRepository
import com.example.infrastructure.kafka.AccountEventProducer
import com.example.service.AccountProjectorService
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.math.BigDecimal
import java.time.Instant
import java.util.*

fun main() = runBlocking {
    val repository = AccountRepository()
    val eventProducer = AccountEventProducer()
    val projectorService = AccountProjectorService(repository)

    // Start the projector service
    projectorService.start()

    // Wait for services to initialize
    delay(5000)

    // Example usage
    val accountId = UUID.randomUUID()
    var version = 0L

    // Create account
    val createCommand = AccountCommand.CreateAccount(accountId)
    eventProducer.publish(
        AccountEvent.AccountCreated(
            accountId = createCommand.accountId,
            version = ++version,
            timestamp = Instant.now()
        )
    )

    // Deposit money
    val depositCommand = AccountCommand.DepositMoney(accountId, BigDecimal("100.00"))
    eventProducer.publish(
        AccountEvent.MoneyDeposited(
            accountId = depositCommand.accountId,
            amount = depositCommand.amount,
            version = ++version,
            timestamp = Instant.now()
        )
    )

    // Withdraw money
    val withdrawCommand = AccountCommand.WithdrawMoney(accountId, BigDecimal("50.00"))
    eventProducer.publish(
        AccountEvent.MoneyWithdrawn(
            accountId = withdrawCommand.accountId,
            amount = withdrawCommand.amount,
            version = ++version,
            timestamp = Instant.now()
        )
    )

    // Wait longer for the projector to process events
    delay(5000)

    // Check the current state
    val account = repository.findById(accountId)
    println("Final account state: $account")

    // Cleanup
    projectorService.stop()
    eventProducer.close()
}
