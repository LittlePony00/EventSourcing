package com.example.domain

import java.math.BigDecimal
import java.util.*

sealed class AccountCommand {
    data class CreateAccount(
        val accountId: UUID = UUID.randomUUID()
    ) : AccountCommand()

    data class DepositMoney(
        val accountId: UUID,
        val amount: BigDecimal
    ) : AccountCommand()

    data class WithdrawMoney(
        val accountId: UUID,
        val amount: BigDecimal
    ) : AccountCommand()
} 