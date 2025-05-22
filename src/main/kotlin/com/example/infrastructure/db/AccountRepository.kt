package com.example.infrastructure.db

import com.example.domain.Account
import com.example.domain.AccountStatus
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.sql.Connection
import java.util.*

class AccountRepository {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val dataSource = createDataSource()

    init {
        createTableIfNotExists()
        logger.info("AccountRepository initialized")
    }

    private fun createDataSource(): HikariDataSource {
        logger.info("Creating database connection pool")
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:postgresql://localhost:25432/bank"
            username = "postgres"
            password = "postgres"
            maximumPoolSize = 10
        }
        return HikariDataSource(config)
    }

    private fun createTableIfNotExists() {
        val sql = """
            CREATE TABLE IF NOT EXISTS accounts (
                id UUID PRIMARY KEY,
                balance DECIMAL NOT NULL,
                status VARCHAR(20) NOT NULL,
                version BIGINT NOT NULL
            )
        """.trimIndent()

        dataSource.connection.use { conn ->
            conn.createStatement().execute(sql)
            logger.info("Accounts table created or verified")
        }
    }

    fun save(account: Account) {
        val sql = """
            INSERT INTO accounts (id, balance, status, version)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE
            SET balance = ?, status = ?, version = ?
        """.trimIndent()

        dataSource.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, account.id)
                stmt.setBigDecimal(2, account.balance)
                stmt.setString(3, account.status.name)
                stmt.setLong(4, account.version)
                stmt.setBigDecimal(5, account.balance)
                stmt.setString(6, account.status.name)
                stmt.setLong(7, account.version)
                val updated = stmt.executeUpdate()
                logger.info("Saved account state: $account, rows affected: $updated")
            }
        }
    }

    fun findById(id: UUID): Account? {
        val sql = "SELECT * FROM accounts WHERE id = ?"
        
        return dataSource.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, id)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    Account(
                        id = rs.getObject("id", UUID::class.java),
                        balance = rs.getBigDecimal("balance"),
                        status = AccountStatus.valueOf(rs.getString("status")),
                        version = rs.getLong("version")
                    ).also {
                        logger.info("Found account: $it")
                    }
                } else {
                    logger.info("Account not found: $id")
                    null
                }
            }
        }
    }
} 