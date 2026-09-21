package org.apache.cassandra.easystress.workloads

import io.mockk.mockk
import com.datastax.oss.driver.api.core.CqlSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MixedTest {
    private val workload = Mixed()

    @Test
    fun `schema returns 27 DDL statements`() {
        assertThat(workload.schema()).hasSize(27)
    }

    @Test
    fun `schema contains IF NOT EXISTS for all tables`() {
        val ddl = workload.schema()
        assertThat(ddl).allSatisfy { stmt ->
            assertThat(stmt).containsIgnoringCase("IF NOT EXISTS")
        }
    }

    @Test
    fun `schema contains all 27 expected table names`() {
        val names = (1..28).map { "table$it" } - setOf("table9")
        val combined = workload.schema().joinToString(" ")
        names.forEach { name ->
            assertThat(combined).containsIgnoringCase(name)
        }
    }
}
