package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import io.mockk.every
import io.mockk.mockk
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.commands.Run
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
    fun `schema returns 10 DDL statements`() {
        assertThat(workload.schema()).hasSize(10)
    }

    @Test
    fun `schema contains IF NOT EXISTS for all tables`() {
        val ddl = workload.schema()
        assertThat(ddl).allSatisfy { stmt ->
            assertThat(stmt).containsIgnoringCase("IF NOT EXISTS")
        }
    }

    @Test
    fun `schema contains all 10 active table names`() {
        val names = (1..10).map { "table$it" }
        val combined = workload.schema().joinToString(" ")
        names.forEach { name ->
            assertThat(combined).containsIgnoringCase(name)
        }
    }

    @Test
    fun `getDefaultReadRate returns 0_4311`() {
        assertThat(workload.getDefaultReadRate()).isEqualTo(0.4311)
    }

    @Test
    fun `partitionCountFactor defaults to 1_0`() {
        assertThat(workload.partitionCountFactor).isEqualTo(1.0)
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

    @Test
    fun `dataSizeFactor defaults to 1_0`() {
        assertThat(workload.dataSizeFactor).isEqualTo(1.0)
    }

    @Test
    fun `getNextMutation returns a Mutation when dataSizeFactor is 0_5`() {
        workload.dataSizeFactor = 0.5
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val op = runner.getNextMutation(PartitionKey("test", 1L))
        assertThat(op).isInstanceOf(Operation.Mutation::class.java)
    }

    @Test
    fun `getNextMutation returns a Mutation when partitionCountFactor is 0_5`() {
        workload.partitionCountFactor = 0.5
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val op = runner.getNextMutation(PartitionKey("test", 1L))
        assertThat(op).isInstanceOf(Operation.Mutation::class.java)
    }

    @Test
    fun `all operations return correct types across 1000 iterations`() {
        workload.prepare(mockSession())
        val runner = workload.getRunner(mockk<StressContext>(relaxed = true))
        val key = PartitionKey("test", 1L)
        repeat(1000) {
            assertThat(runner.getNextSelect(key)).isInstanceOf(Operation.SelectStatement::class.java)
            assertThat(runner.getNextMutation(key)).isInstanceOf(Operation.Mutation::class.java)
            assertThat(runner.getNextDelete(key)).isInstanceOf(Operation.Deletion::class.java)
        }
    }

    @Test
    fun `readRate 0_0 is honoured — getRunner returns a working runner`() {
        val args = mockk<Run>(relaxed = true)
        every { args.readRate } returns 0.0
        val context = mockk<StressContext>(relaxed = true)
        every { context.mainArguments } returns args

        workload.prepare(mockSession())
        val runner = workload.getRunner(context)
        // Runner must still produce valid mutations — reads are just never requested by WorkloadRunner
        assertThat(runner.getNextMutation(PartitionKey("test", 1L))).isInstanceOf(Operation.Mutation::class.java)
    }
}
