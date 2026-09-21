package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import io.mockk.every
import io.mockk.mockk
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MixedTest {
    private val workload = Mixed()

    /** Returns a CqlSession mock where every prepare() call returns a PreparedStatement
     *  whose bind() returns a BoundStatement mock that chains all setter calls back to itself. */
    private fun mockSession(): CqlSession {
        val bound = mockk<BoundStatement>(relaxed = true)
        every { bound.setBoolean(any<Int>(), any()) } returns bound
        every { bound.setByte(any<Int>(), any()) } returns bound
        every { bound.setShort(any<Int>(), any()) } returns bound
        every { bound.setInt(any<Int>(), any()) } returns bound
        every { bound.setLong(any<Int>(), any()) } returns bound
        every { bound.setFloat(any<Int>(), any()) } returns bound
        every { bound.setDouble(any<Int>(), any()) } returns bound
        every { bound.setInstant(any<Int>(), any()) } returns bound
        every { bound.setString(any<Int>(), any()) } returns bound
        every { bound.setUuid(any<Int>(), any()) } returns bound
        every { bound.setByteBuffer(any<Int>(), any()) } returns bound
        every { bound.setMap<String, Long>(any<Int>(), any(), any<Class<String>>(), any<Class<Long>>()) } returns bound

        val ps = mockk<PreparedStatement>(relaxed = true)
        every { ps.bind() } returns bound

        val session = mockk<CqlSession>(relaxed = true)
        every { session.prepare(any<String>()) } returns ps
        return session
    }

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

    @Test
    fun `getDefaultReadRate returns 0_42`() {
        assertThat(workload.getDefaultReadRate()).isEqualTo(0.42)
    }

    @Test
    fun `getNextSelect returns a SelectStatement operation`() {
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val op = runner.getNextSelect(PartitionKey("test", 1L))
        assertThat(op).isInstanceOf(Operation.SelectStatement::class.java)
    }

    @Test
    fun `getNextMutation returns a Mutation operation`() {
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val op = runner.getNextMutation(PartitionKey("test", 1L))
        assertThat(op).isInstanceOf(Operation.Mutation::class.java)
    }

    @Test
    fun `getNextDelete returns a Deletion operation`() {
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val op = runner.getNextDelete(PartitionKey("test", 1L))
        assertThat(op).isInstanceOf(Operation.Deletion::class.java)
    }
}
