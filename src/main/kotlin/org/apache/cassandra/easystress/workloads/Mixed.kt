package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * Multi-table mixed workload modelling a real Cassandra keyspace traffic shape.
 * 13 active tables are included in the read/write dispatch; 14 zero-traffic tables
 * have their schema emitted so the keyspace structure mirrors production.
 * Per-table read/write ratios and blob sizes are derived from cfstats.
 */
class Mixed : IStressWorkload {

    // ── PreparedStatements ────────────────────────────────────────────────────
    private lateinit var insertTable1: PreparedStatement
    private lateinit var selectTable1: PreparedStatement
    private lateinit var insertTable3: PreparedStatement
    private lateinit var selectTable3: PreparedStatement
    private lateinit var insertTable4: PreparedStatement
    private lateinit var selectTable4: PreparedStatement
    private lateinit var insertTable6: PreparedStatement
    private lateinit var selectTable6: PreparedStatement
    private lateinit var insertTable7: PreparedStatement
    private lateinit var selectTable7: PreparedStatement
    private lateinit var insertTable8: PreparedStatement
    private lateinit var selectTable8: PreparedStatement
    private lateinit var insertTable10: PreparedStatement
    private lateinit var selectTable10: PreparedStatement
    private lateinit var insertTable13: PreparedStatement
    private lateinit var selectTable13: PreparedStatement
    private lateinit var insertTable15: PreparedStatement
    private lateinit var selectTable15: PreparedStatement
    private lateinit var insertTable16: PreparedStatement
    private lateinit var selectTable16: PreparedStatement
    private lateinit var insertTable21: PreparedStatement
    private lateinit var selectTable21: PreparedStatement
    private lateinit var insertTable24: PreparedStatement
    private lateinit var selectTable24: PreparedStatement
    private lateinit var insertTable25: PreparedStatement
    private lateinit var selectTable25: PreparedStatement

    override fun prepare(session: CqlSession) {
        insertTable1  = session.prepare("INSERT INTO table1  (field1,field2,field3,field4,field5,field6,field7,field8) VALUES (?,?,?,?,?,?,?,?)")
        selectTable1  = session.prepare("SELECT * FROM table1  WHERE field1=?")
        insertTable3  = session.prepare("INSERT INTO table3  (field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        selectTable3  = session.prepare("SELECT * FROM table3  WHERE field1=? AND field2=? AND field3=?")
        insertTable4  = session.prepare("INSERT INTO table4  (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable4  = session.prepare("SELECT * FROM table4  WHERE field1=?")
        insertTable6  = session.prepare("INSERT INTO table6  (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable6  = session.prepare("SELECT * FROM table6  WHERE field1=?")
        insertTable7  = session.prepare("INSERT INTO table7  (field1,field2,field3,field4,field5,field6) VALUES (?,?,?,?,?,?)")
        selectTable7  = session.prepare("SELECT * FROM table7  WHERE field1=? AND field2=? AND field3=?")
        insertTable8  = session.prepare("INSERT INTO table8  (field1,field2,field3,field4) VALUES (?,?,?,?)")
        selectTable8  = session.prepare("SELECT * FROM table8  WHERE field1=?")
        insertTable10 = session.prepare("INSERT INTO table10 (field1,field2,field3,field4,field5,field6,field7) VALUES (?,?,?,?,?,?,?)")
        selectTable10 = session.prepare("SELECT * FROM table10 WHERE field1=? AND field2=?")
        insertTable13 = session.prepare("INSERT INTO table13 (field1,field2,field3,field4,field5,field6) VALUES (?,?,?,?,?,?)")
        selectTable13 = session.prepare("SELECT * FROM table13 WHERE field1=?")
        insertTable15 = session.prepare("INSERT INTO table15 (field1,field2,field3,field4,field5,field6,field7,field8) VALUES (?,?,?,?,?,?,?,?)")
        selectTable15 = session.prepare("SELECT * FROM table15 WHERE field1=?")
        insertTable16 = session.prepare("INSERT INTO table16 (field1,field2,field3,field4,field5,field6,field7) VALUES (?,?,?,?,?,?,?)")
        selectTable16 = session.prepare("SELECT * FROM table16 WHERE field1=? AND field2=?")
        insertTable21 = session.prepare("INSERT INTO table21 (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable21 = session.prepare("SELECT * FROM table21 WHERE field1=?")
        insertTable24 = session.prepare("INSERT INTO table24 (field1,field2,field3,field4,field5,field6,field7,field8,field9,field10) VALUES (?,?,?,?,?,?,?,?,?,?)")
        selectTable24 = session.prepare("SELECT * FROM table24 WHERE field1=? AND field2=? AND field3=?")
        insertTable25 = session.prepare("INSERT INTO table25 (field1,field2,field3,field4,field5,field6) VALUES (?,?,?,?,?,?)")
        selectTable25 = session.prepare("SELECT * FROM table25 WHERE field1=?")
    }

    override fun getRunner(context: StressContext): IStressRunner {
        // Cumulative read thresholds (index matches tableRunners order below)
        val readThresholds = doubleArrayOf(
            0.6574,                      // table16
            0.8122,                      // table15
            0.8836,                      // table6
            0.9665,                      // table4
            0.9943,                      // table24
            0.9962,                      // table25
            0.9998,                      // table10
            0.9999,                      // table3
            0.99996,                     // table13
            0.999966,                    // table21
            0.999970,                    // table1
            0.999970 + 0.0000027,        // table7
            1.0,                         // table8
        )
        // Cumulative write thresholds
        val writeThresholds = doubleArrayOf(
            0.4947,                      // table16
            0.6548,                      // table10
            0.8362,                      // table15
            0.9764,                      // table4
            0.9829,                      // table1
            0.9968,                      // table24
            0.9989,                      // table6
            0.9998,                      // table25
            0.99982,                     // table3
            0.99983,                     // table13
            0.99983 + 0.0000001,         // table8
            0.99983 + 0.0000002,         // table7
            1.0,                         // table21
        )

        return object : IStressRunner {
            private val rng = ThreadLocalRandom.current()

            private fun pick(thresholds: DoubleArray): Int {
                val r = rng.nextDouble()
                thresholds.forEachIndexed { i, t -> if (r < t) return i }
                return thresholds.size - 1
            }

            private fun blob(minBytes: Int, maxBytes: Int): ByteBuffer {
                val size = if (minBytes >= maxBytes) minBytes else rng.nextInt(minBytes, maxBytes + 1)
                return ByteBuffer.wrap(ByteArray(size).also { rng.nextBytes(it) })
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound = when (pick(readThresholds)) {
                    0  -> selectTable16.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText())
                    1  -> selectTable15.bind().setUuid(0, UUID.randomUUID())
                    2  -> selectTable6.bind().setInt(0, partitionKey.getText().hashCode())
                    3  -> selectTable4.bind().setUuid(0, UUID.randomUUID())
                    4  -> selectTable24.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    5  -> selectTable25.bind().setInt(0, partitionKey.getText().hashCode())
                    6  -> selectTable10.bind().setInt(0, partitionKey.getText().hashCode()).setLong(1, rng.nextLong())
                    7  -> selectTable3.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    8  -> selectTable13.bind().setUuid(0, UUID.randomUUID())
                    9  -> selectTable21.bind().setInt(0, partitionKey.getText().hashCode())
                    10 -> selectTable1.bind().setInt(0, partitionKey.getText().hashCode())
                    11 -> selectTable7.bind().setInt(0, rng.nextInt()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    else -> selectTable8.bind().setString(0, partitionKey.getText())
                }
                return Operation.SelectStatement(bound)
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val bound = when (pick(writeThresholds)) {
                    0  -> insertTable16.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setString(1, partitionKey.getText())
                            .setUuid(2, UUID.randomUUID())
                            .setInt(3, rng.nextInt())
                            .setLong(4, rng.nextLong())
                            .setByteBuffer(5, blob(150, 62_479_625))
                            .setString(6, "json")
                    1  -> insertTable10.bind()
                            .setInt(0, partitionKey.getText().hashCode())
                            .setLong(1, rng.nextLong())
                            .setInt(2, rng.nextInt())
                            .setLong(3, rng.nextLong())
                            .setLong(4, rng.nextLong())
                            .setByteBuffer(5, blob(73, 2_346_799))
                            .setString(6, "json")
                    2  -> insertTable15.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setUuid(1, UUID.randomUUID())
                            .setLong(2, rng.nextLong())
                            .setLong(3, rng.nextLong())
                            .setByteBuffer(4, blob(87, 25_109_160))
                            .setString(5, "json")
                            .setLong(6, rng.nextLong())
                            .setLong(7, rng.nextLong())
                    3  -> insertTable4.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setUuid(1, UUID.randomUUID())
                            .setByteBuffer(2, blob(51, 5_722))
                            .setString(3, "json")
                            .setLong(4, rng.nextLong())
                    4  -> insertTable1.bind()
                            .setInt(0, partitionKey.getText().hashCode())
                            .setLong(1, rng.nextLong())
                            .setString(2, partitionKey.getText())
                            .setByteBuffer(3, blob(373, 785_939))
                            .setString(4, "json")
                            .setLong(5, rng.nextLong())
                            .setString(6, partitionKey.getText())
                            .setLong(7, rng.nextLong())
                    5  -> insertTable24.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setString(1, partitionKey.getText())
                            .setInt(2, rng.nextInt())
                            .setInt(3, rng.nextInt())
                            .setLong(4, rng.nextLong())
                            .setLong(5, rng.nextLong())
                            .setByteBuffer(6, blob(125, 668_489_532))
                            .setString(7, "json")
                            .setByteBuffer(8, blob(125, 668_489_532))
                            .setString(9, "json")
                    6  -> insertTable6.bind()
                            .setInt(0, partitionKey.getText().hashCode())
                            .setInt(1, rng.nextInt())
                            .setLong(2, rng.nextLong())
                            .setByteBuffer(3, blob(771, 129_557_750))
                            .setString(4, "json")
                    7  -> insertTable25.bind()
                            .setInt(0, partitionKey.getText().hashCode())
                            .setInt(1, rng.nextInt())
                            .setLong(2, rng.nextLong())
                            .setLong(3, rng.nextLong())
                            .setByteBuffer(4, blob(3_974, 29_521))
                            .setString(5, "json")
                    8  -> insertTable3.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setString(1, partitionKey.getText())
                            .setInt(2, rng.nextInt())
                            .setInt(3, rng.nextInt())
                            .setLong(4, rng.nextLong())
                            .setLong(5, rng.nextLong())
                            .setLong(6, rng.nextLong())
                            .setLong(7, rng.nextLong())
                            .setByteBuffer(8, blob(180, 25_109_160))
                            .setString(9, "json")
                            .setByteBuffer(10, blob(180, 25_109_160))
                            .setString(11, "json")
                    9  -> insertTable13.bind()
                            .setUuid(0, UUID.randomUUID())
                            .setString(1, partitionKey.getText())
                            .setString(2, partitionKey.getText())
                            .setByteBuffer(3, blob(50, 200))
                            .setString(4, "json")
                            .setLong(5, rng.nextLong())
                    10 -> insertTable8.bind()
                            .setString(0, partitionKey.getText())
                            .setInstant(1, Instant.now())
                            .setString(2, partitionKey.getText())
                            .setString(3, partitionKey.getText())
                    11 -> insertTable7.bind()
                            .setInt(0, rng.nextInt())
                            .setString(1, partitionKey.getText())
                            .setInt(2, rng.nextInt())
                            .setLong(3, rng.nextLong())
                            .setString(4, "json")
                            .setByteBuffer(5, blob(1_917, 2_299))
                    else -> insertTable21.bind()
                            .setInt(0, partitionKey.getText().hashCode())
                            .setMap(1, emptyMap<String, Long>(), String::class.java, Long::class.javaObjectType)
                            .setByteBuffer(2, blob(50, 200))
                            .setString(3, "json")
                            .setLong(4, rng.nextLong())
                }
                return Operation.Mutation(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                // Delete uses the same table selection as writes (write-weighted)
                val bound = when (pick(writeThresholds)) {
                    0  -> selectTable16.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText())
                    1  -> selectTable10.bind().setInt(0, partitionKey.getText().hashCode()).setLong(1, rng.nextLong())
                    2  -> selectTable15.bind().setUuid(0, UUID.randomUUID())
                    3  -> selectTable4.bind().setUuid(0, UUID.randomUUID())
                    4  -> selectTable1.bind().setInt(0, partitionKey.getText().hashCode())
                    5  -> selectTable24.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    6  -> selectTable6.bind().setInt(0, partitionKey.getText().hashCode())
                    7  -> selectTable25.bind().setInt(0, partitionKey.getText().hashCode())
                    8  -> selectTable3.bind().setUuid(0, UUID.randomUUID()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    9  -> selectTable13.bind().setUuid(0, UUID.randomUUID())
                    10 -> selectTable8.bind().setString(0, partitionKey.getText())
                    11 -> selectTable7.bind().setInt(0, rng.nextInt()).setString(1, partitionKey.getText()).setInt(2, rng.nextInt())
                    else -> selectTable21.bind().setInt(0, partitionKey.getText().hashCode())
                }
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getDefaultReadRate() = 0.42

    // ── schema ────────────────────────────────────────────────────────────────
    override fun schema(): List<String> = activeTableDDL + zeroTrafficTableDDL

    // 13 active tables
    private val activeTableDDL = listOf(
        // table1: PK=(field1 int), CK=(field2 bigint DESC, field3 text ASC)
        """CREATE TABLE IF NOT EXISTS table1 (
            field1 int,
            field2 bigint,
            field3 text,
            field4 blob,
            field5 text,
            field6 bigint,
            field7 text,
            field8 bigint,
            PRIMARY KEY (field1, field2, field3)
        ) WITH CLUSTERING ORDER BY (field2 DESC, field3 ASC)""",

        // table3: composite PK=(field1 uuid,field2 text,field3 int), CK=(field4 int ASC, field5 bigint ASC, field6 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table3 (
            field1 uuid,
            field2 text,
            field3 int,
            field4 int,
            field5 bigint,
            field6 bigint,
            field7 bigint,
            field8 bigint,
            field9 blob,
            field10 text,
            field11 blob,
            field12 text,
            PRIMARY KEY ((field1, field2, field3), field4, field5, field6)
        ) WITH CLUSTERING ORDER BY (field4 ASC, field5 ASC, field6 ASC)""",

        // table4: PK=(field1 uuid), CK=(field2 uuid ASC)
        """CREATE TABLE IF NOT EXISTS table4 (
            field1 uuid,
            field2 uuid,
            field3 blob,
            field4 text,
            field5 bigint,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",

        // table6: PK=(field1 int), CK=(field2 int ASC, field3 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table6 (
            field1 int,
            field2 int,
            field3 bigint,
            field4 blob,
            field5 text,
            PRIMARY KEY (field1, field2, field3)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC)""",

        // table7: composite PK=(field1 int,field2 text,field3 int), CK=(field4 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table7 (
            field1 int,
            field2 text,
            field3 int,
            field4 bigint,
            field5 text,
            field6 blob,
            PRIMARY KEY ((field1, field2, field3), field4)
        ) WITH CLUSTERING ORDER BY (field4 ASC)""",

        // table8: simple PK=field1 text
        """CREATE TABLE IF NOT EXISTS table8 (
            field1 text PRIMARY KEY,
            field2 timestamp,
            field3 text,
            field4 text
        )""",

        // table10: composite PK=(field1 int,field2 bigint), CK=(field3 int ASC, field4 bigint ASC, field5 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table10 (
            field1 int,
            field2 bigint,
            field3 int,
            field4 bigint,
            field5 bigint,
            field6 blob,
            field7 text,
            PRIMARY KEY ((field1, field2), field3, field4, field5)
        ) WITH CLUSTERING ORDER BY (field3 ASC, field4 ASC, field5 ASC)""",

        // table13: PK=(field1 uuid), CK=(field2 text ASC, field3 text ASC)
        """CREATE TABLE IF NOT EXISTS table13 (
            field1 uuid,
            field2 text,
            field3 text,
            field4 blob,
            field5 text,
            field6 bigint,
            PRIMARY KEY (field1, field2, field3)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC)""",

        // table15: PK=(field1 uuid), CK=(field2 uuid ASC, field3 bigint ASC, field4 bigint DESC)
        """CREATE TABLE IF NOT EXISTS table15 (
            field1 uuid,
            field2 uuid,
            field3 bigint,
            field4 bigint,
            field5 blob,
            field6 text,
            field7 bigint,
            field8 bigint,
            PRIMARY KEY (field1, field2, field3, field4)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 DESC)""",

        // table16: composite PK=(field1 uuid,field2 text), CK=(field3 uuid ASC, field4 int ASC, field5 bigint DESC)
        """CREATE TABLE IF NOT EXISTS table16 (
            field1 uuid,
            field2 text,
            field3 uuid,
            field4 int,
            field5 bigint,
            field6 blob,
            field7 text,
            PRIMARY KEY ((field1, field2), field3, field4, field5)
        ) WITH CLUSTERING ORDER BY (field3 ASC, field4 ASC, field5 DESC)""",

        // table21: simple PK=field1 int
        """CREATE TABLE IF NOT EXISTS table21 (
            field1 int PRIMARY KEY,
            field2 map<text, bigint>,
            field3 blob,
            field4 text,
            field5 bigint
        )""",

        // table24: composite PK=(field1 uuid,field2 text,field3 int), CK=(field4 int ASC, field5 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table24 (
            field1 uuid,
            field2 text,
            field3 int,
            field4 int,
            field5 bigint,
            field6 bigint,
            field7 blob,
            field8 text,
            field9 blob,
            field10 text,
            PRIMARY KEY ((field1, field2, field3), field4, field5)
        ) WITH CLUSTERING ORDER BY (field4 ASC, field5 ASC)""",

        // table25: PK=(field1 int), CK=(field2 int ASC, field3 bigint ASC, field4 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table25 (
            field1 int,
            field2 int,
            field3 bigint,
            field4 bigint,
            field5 blob,
            field6 text,
            PRIMARY KEY (field1, field2, field3, field4)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 ASC)""",
    )

    // 14 zero-traffic tables — schema only, no runners
    private val zeroTrafficTableDDL = listOf(
        // table2: PK=(field1 uuid), CK=(field2 uuid ASC, field3 bigint ASC, field4 bigint DESC)
        """CREATE TABLE IF NOT EXISTS table2 (
            field1 uuid,
            field2 uuid,
            field3 bigint,
            field4 bigint,
            field5 blob,
            field6 text,
            field7 bigint,
            PRIMARY KEY (field1, field2, field3, field4)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 DESC)""",

        // table5: PK=(field1 uuid), CK=(field2 uuid ASC)
        """CREATE TABLE IF NOT EXISTS table5 (
            field1 uuid,
            field2 uuid,
            field3 blob,
            field4 text,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",

        // table11: PK=(field1 tinyint), CK=(field2 tinyint ASC, field3 uuid ASC)
        """CREATE TABLE IF NOT EXISTS table11 (
            field1 tinyint,
            field2 tinyint,
            field3 uuid,
            field4 timestamp,
            field5 inet,
            field6 smallint,
            field7 timestamp,
            PRIMARY KEY (field1, field2, field3)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC)""",

        // table12: PK=(field1 int), CK=(field2 timestamp ASC, field3 uuid ASC, field4 uuid ASC, field5 timestamp ASC, field6 tinyint ASC)
        """CREATE TABLE IF NOT EXISTS table12 (
            field1 int,
            field2 timestamp,
            field3 uuid,
            field4 uuid,
            field5 timestamp,
            field6 tinyint,
            field7 tinyint,
            field8 map<text, timestamp>,
            field9 bigint,
            PRIMARY KEY (field1, field2, field3, field4, field5, field6)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 ASC, field5 ASC, field6 ASC)""",

        // table14: simple PK=field1 int
        """CREATE TABLE IF NOT EXISTS table14 (
            field1 int PRIMARY KEY,
            field2 blob,
            field3 text,
            field4 bigint,
            field5 bigint
        )""",

        // table17: composite PK=(field1 int,field2 int), CK=(field3 timestamp ASC)
        """CREATE TABLE IF NOT EXISTS table17 (
            field1 int,
            field2 int,
            field3 timestamp,
            field4 text,
            field5 text,
            field6 text,
            field7 text,
            PRIMARY KEY ((field1, field2), field3)
        ) WITH CLUSTERING ORDER BY (field3 ASC)""",

        // table18: PK=(field1 int), CK=(field2 text ASC)
        """CREATE TABLE IF NOT EXISTS table18 (
            field1 int,
            field2 text,
            field3 blob,
            field4 text,
            field5 bigint,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",

        // table19: PK=(field1 int), CK=(field2 bigint ASC)
        """CREATE TABLE IF NOT EXISTS table19 (
            field1 int,
            field2 bigint,
            field3 text,
            field4 blob,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",

        // table20: composite PK=(field1 int,field2 text)
        """CREATE TABLE IF NOT EXISTS table20 (
            field1 int,
            field2 text,
            field3 text,
            field4 blob,
            field5 bigint,
            PRIMARY KEY ((field1, field2))
        )""",

        // table22: PK=(field1 int), CK=(field2 text ASC)
        """CREATE TABLE IF NOT EXISTS table22 (
            field1 int,
            field2 text,
            field3 blob,
            field4 text,
            field5 uuid,
            field6 boolean,
            field7 bigint,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",

        // table23: composite PK=(field1 int,field2 int), CK=(field3 bigint DESC, field4 text ASC)
        """CREATE TABLE IF NOT EXISTS table23 (
            field1 int,
            field2 int,
            field3 bigint,
            field4 text,
            field5 blob,
            field6 text,
            field7 bigint,
            field8 bigint,
            PRIMARY KEY ((field1, field2), field3, field4)
        ) WITH CLUSTERING ORDER BY (field3 DESC, field4 ASC)""",

        // table26: simple PK=field1 int
        """CREATE TABLE IF NOT EXISTS table26 (
            field1 int PRIMARY KEY,
            field2 blob,
            field3 text,
            field4 bigint
        )""",

        // table27: simple PK=field1 int
        """CREATE TABLE IF NOT EXISTS table27 (
            field1 int PRIMARY KEY,
            field2 blob,
            field3 text,
            field4 bigint
        )""",

        // table28: simple PK=field1 uuid
        """CREATE TABLE IF NOT EXISTS table28 (
            field1 uuid PRIMARY KEY,
            field2 text
        )""",
    )
}
