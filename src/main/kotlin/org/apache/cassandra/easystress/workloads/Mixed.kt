package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.WorkloadParameter
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * Multi-table mixed workload modelling a real Cassandra keyspace traffic shape.
 * 10 active tables, numbered by descending combined traffic volume (table1 = highest traffic).
 * Per-table read/write ratios and partition size distributions are derived from cfstats.
 * Per-table partition pools and row-count distributions are derived from cfhistograms.
 *
 * Each table uses an independent pool-based key generator rather than the shared partitionKey
 * argument (which is ignored, like --readrate). Pool sizes are scaled by partitionCountFactor.
 */
class Mixed : IStressWorkload {
    // Ensures the --readrate warning is printed only once across all threads
    @Volatile private var warnedAboutReadRate = false

    // Ensures the --partitioncount warning is printed only once across all threads
    @Volatile private var warnedAboutPartitionCount = false

    @WorkloadParameter("Data size factor relative to cfstats baseline. 1.0 = unchanged, 0.5 = half size, 1.5 = 50% larger.")
    var dataSizeFactor: Double = 1.0

    @WorkloadParameter("Partition count factor relative to cfhistograms baseline. 1.0 = unchanged, 0.5 = half partitions, 2.0 = double.")
    var partitionCountFactor: Double = 1.0

    // ── PreparedStatements ────────────────────────────────────────────────────
    private lateinit var insertTable1: PreparedStatement
    private lateinit var selectTable1: PreparedStatement
    private lateinit var insertTable2: PreparedStatement
    private lateinit var selectTable2: PreparedStatement
    private lateinit var insertTable3: PreparedStatement
    private lateinit var selectTable3: PreparedStatement
    private lateinit var insertTable4: PreparedStatement
    private lateinit var selectTable4: PreparedStatement
    private lateinit var insertTable5: PreparedStatement
    private lateinit var selectTable5: PreparedStatement
    private lateinit var insertTable6: PreparedStatement
    private lateinit var selectTable6: PreparedStatement
    private lateinit var insertTable7: PreparedStatement
    private lateinit var selectTable7: PreparedStatement
    private lateinit var insertTable8: PreparedStatement
    private lateinit var selectTable8: PreparedStatement
    private lateinit var insertTable9: PreparedStatement
    private lateinit var selectTable9: PreparedStatement
    private lateinit var insertTable10: PreparedStatement
    private lateinit var selectTable10: PreparedStatement

    // DELETE statements — one per active table
    private lateinit var deleteTable1: PreparedStatement
    private lateinit var deleteTable2: PreparedStatement
    private lateinit var deleteTable3: PreparedStatement
    private lateinit var deleteTable4: PreparedStatement
    private lateinit var deleteTable5: PreparedStatement
    private lateinit var deleteTable6: PreparedStatement
    private lateinit var deleteTable7: PreparedStatement
    private lateinit var deleteTable8: PreparedStatement
    private lateinit var deleteTable9: PreparedStatement
    private lateinit var deleteTable10: PreparedStatement

    override fun prepare(session: CqlSession) {
        // table1: composite PK=(field1 uuid,field2 text)
        insertTable1 =
            session.prepare(
                "INSERT INTO table1 (field1,field2,field3,field4,field5,field6,field7) VALUES (?,?,?,?,?,?,?)",
            )
        selectTable1 = session.prepare("SELECT * FROM table1 WHERE field1=? AND field2=?")
        // table2: PK=(field1 uuid)
        insertTable2 =
            session.prepare(
                "INSERT INTO table2 (field1,field2,field3,field4,field5,field6,field7,field8) VALUES (?,?,?,?,?,?,?,?)",
            )
        selectTable2 = session.prepare("SELECT * FROM table2 WHERE field1=?")
        // table3: PK=(field1 uuid)
        insertTable3 =
            session.prepare("INSERT INTO table3 (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable3 = session.prepare("SELECT * FROM table3 WHERE field1=?")
        // table4: composite PK=(field1 int,field2 bigint)
        insertTable4 =
            session.prepare("INSERT INTO table4 (field1,field2,field3,field4,field5,field6,field7) VALUES (?,?,?,?,?,?,?)")
        selectTable4 = session.prepare("SELECT * FROM table4 WHERE field1=? AND field2=?")
        // table5: PK=(field1 int)
        insertTable5 =
            session.prepare("INSERT INTO table5 (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable5 = session.prepare("SELECT * FROM table5 WHERE field1=?")
        // table6: composite PK=(field1 uuid,field2 text,field3 int), CK=(field4 int,field5 bigint)
        // Always exactly 3 rows per partition — CK cycles over fixed values {0,1,2}
        insertTable6 =
            session.prepare(
                "INSERT INTO table6 (field1,field2,field3,field4,field5,field6,field7,field8,field9,field10)" +
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
            )
        selectTable6 = session.prepare("SELECT * FROM table6 WHERE field1=? AND field2=? AND field3=?")
        // table7: PK=(field1 int)
        insertTable7 =
            session.prepare("INSERT INTO table7 (field1,field2,field3,field4,field5,field6,field7,field8) VALUES (?,?,?,?,?,?,?,?)")
        selectTable7 = session.prepare("SELECT * FROM table7 WHERE field1=?")
        // table8: PK=(field1 int)
        insertTable8 =
            session.prepare(
                "INSERT INTO table8 (field1,field2,field3,field4,field5,field6) VALUES (?,?,?,?,?,?)",
            )
        selectTable8 = session.prepare("SELECT * FROM table8 WHERE field1=?")
        // table9: simple PK=field1 int, CK=(field2 bigint) — always exactly 3 rows, CK cycles {0,1,2}
        insertTable9 =
            session.prepare("INSERT INTO table9 (field1,field2,field3,field4,field5) VALUES (?,?,?,?,?)")
        selectTable9 = session.prepare("SELECT * FROM table9 WHERE field1=?")
        // table10: composite PK=(field1 int,field2 text,field3 int), CK=(field4 bigint)
        // Always exactly 3 rows per partition — CK cycles over fixed values {0,1,2}
        insertTable10 =
            session.prepare("INSERT INTO table10 (field1,field2,field3,field4,field5,field6) VALUES (?,?,?,?,?,?)")
        selectTable10 = session.prepare("SELECT * FROM table10 WHERE field1=? AND field2=? AND field3=?")
        deleteTable1 = session.prepare("DELETE FROM table1  WHERE field1=? AND field2=?")
        deleteTable2 = session.prepare("DELETE FROM table2  WHERE field1=?")
        deleteTable3 = session.prepare("DELETE FROM table3  WHERE field1=?")
        deleteTable4 = session.prepare("DELETE FROM table4  WHERE field1=? AND field2=?")
        deleteTable5 = session.prepare("DELETE FROM table5  WHERE field1=?")
        deleteTable6 = session.prepare("DELETE FROM table6  WHERE field1=? AND field2=? AND field3=?")
        deleteTable7 = session.prepare("DELETE FROM table7  WHERE field1=?")
        deleteTable8 = session.prepare("DELETE FROM table8  WHERE field1=?")
        deleteTable9 = session.prepare("DELETE FROM table9  WHERE field1=?")
        deleteTable10 = session.prepare("DELETE FROM table10 WHERE field1=? AND field2=? AND field3=?")
    }

    override fun getRunner(context: StressContext): IStressRunner {
        if (!warnedAboutReadRate && context.mainArguments.readRate != null && context.mainArguments.readRate != 0.0) {
            println(
                "WARNING: Mixed workload uses a predefined read/write ratio across target tables (read rate = ${getDefaultReadRate()}). " +
                    "The --readrate override is ignored. Use --readrate 0 to disable reads entirely.",
            )
            warnedAboutReadRate = true
        }
        if (!warnedAboutPartitionCount && context.mainArguments.partitionCount != 100_000L) {
            println(
                "WARNING: Mixed workload uses per-table partition pools derived from cfhistograms. " +
                    "The --partitioncount override is ignored. Use --partitioncountfactor instead.",
            )
            warnedAboutPartitionCount = true
        }

        // ── Per-table partition pool sizes (scaled by partitionCountFactor) ──────────
        // Derivation: pools sized so that at steady state each table reaches the row-count
        // percentiles observed in cfhistograms (reference: 100M total writes).
        // All pool sizes are relative; scale with partitionCountFactor for smaller/larger clusters.
        val f = partitionCountFactor

        // table1: bimodal — P50=1 row, P99=4 rows, Max=152K
        // hot=0.5% of writes → 10 keys accumulate ~23K rows each (< 152K max)
        // cold=99.5% of writes → 46M keys stay at ~1 row each
        val t1Cold = maxOf(1L, (46_000_000 * f).toLong())
        val t1Hot  = maxOf(1L, (10 * f).toLong())

        // table2: P50=1 row, P75=12, P99=24, Max=73K
        // hot=1% of writes → 10 keys accumulate ~18K rows each (< 73K max)
        // cold=99% of writes → 18M keys stay at ~1 row each
        val t2Cold = maxOf(1L, (18_000_000 * f).toLong())
        val t2Hot  = maxOf(1L, (10 * f).toLong())

        // table3: near-flat — P50=1, P75=3, Max=35
        // single pool targeting P75=3 rows
        val t3Pool = maxOf(1L, (4_000_000 * f).toLong())

        // table4: strongly multi-row — P50=4, P75=42, P95=1916, Max=35K
        // three pools: bulk 90% → P50=4; mid 9% → P75=42; heavy 1% → P99=2299
        val t4Bulk   = maxOf(1L, (4_000_000 * f).toLong())
        val t4Mid    = maxOf(1L, (40_000 * f).toLong())
        val t4Heavy  = maxOf(1L, (100 * f).toLong())

        // table5: extreme bimodal — P50=1 (cold, ~219 KB/row), P75=61K rows (hot, ~165 bytes/row)
        // hot=25% of writes → 10 keys accumulate ~60K small rows each
        // cold=75% of writes → 1.8M keys each hold 1 large row
        val t5Cold = maxOf(1L, (1_800_000 * f).toLong())
        val t5Hot  = maxOf(1L, (10 * f).toLong())

        // table6: fixed 3 rows/partition — P50=P99=3, Max=3
        // pool of ~400 partitions; CK cycles over {0,1,2} to hold row count constant
        val t6Pool = maxOf(1L, (400 * f).toLong())

        // table7: always large — P50=1109, P75=6866, Max=8239
        // single small pool of ~400 keys, each accumulates 1K–8K rows
        val t7Pool = maxOf(1L, (400 * f).toLong())

        // table8: always exactly 1 row/partition — P50=P99=1
        // pool of 50K distinct keys; each has exactly 1 row (CK fixed per insert)
        val t8Pool = maxOf(1L, (50_000 * f).toLong())

        // table9: near-fixed 3 rows — P50=P99=3, Max=20K
        // small pool; CK cycles over {0,1,2} to stabilise at 3 rows/partition
        val t9Pool = maxOf(1L, (30 * f).toLong())

        // table10: near-fixed 3 rows — P50=P99=3, Max=379K
        // pool of ~43K partitions; CK cycles over {0,1,2}
        val t10Pool = maxOf(1L, (43_000 * f).toLong())

        // Cumulative read thresholds — tables ordered by descending read share (cfstats)
        // table1, table5, table2, table3, table8, table4, table10, table6, table9, table7
        val readThresholds =
            doubleArrayOf(
                0.6561806857,
                0.7819905903,
                0.9002315580,
                0.9784406932,
                0.9879104317,
                0.9971003682,
                0.9999689904,
                0.9999900486,
                0.9999984273,
                1.0,
            )

        // Cumulative write thresholds — tables ordered by descending write share (cfstats)
        // table1, table2, table4, table3, table5, table7, table10, table8, table6, table9
        val writeThresholds =
            doubleArrayOf(
                0.4658071045,
                0.6519643056,
                0.8378270691,
                0.9693986288,
                0.9937551121,
                0.9981212874,
                0.9994640927,
                0.9999951549,
                0.9999990596,
                1.0,
            )

        return object : IStressRunner {
            private fun pick(thresholds: DoubleArray): Int {
                val rng = ThreadLocalRandom.current()
                val r = rng.nextDouble()
                thresholds.forEachIndexed { i, t -> if (r < t) return i }
                return thresholds.size - 1
            }

            /**
             * Generates a random blob whose size follows a shifted exponential distribution,
             * matching the real partition size distribution observed in cfstats.
             * The distribution has the correct mean and respects [pmin, pmax] bounds.
             */
            private fun blob(
                pmin: Int,
                pmean: Int,
                pmax: Int,
            ): ByteBuffer {
                val rng = ThreadLocalRandom.current()
                val min = maxOf(1, (pmin * dataSizeFactor).toInt())
                val max = maxOf(min, (pmax * dataSizeFactor).toInt())
                val mean = maxOf(min, minOf(max, (pmean * dataSizeFactor).toInt()))

                val size =
                    if (min >= max || mean <= min) {
                        min
                    } else {
                        // Shifted Exp(λ) where λ = 1/(mean-min): produces correct mean,
                        // decays exponentially toward pmax — matches real partition size skew.
                        val expSample = (-Math.log(1.0 - rng.nextDouble()) * (mean - min)).toLong()
                        (min + expSample).coerceAtMost(max.toLong()).toInt()
                    }

                return ByteBuffer.wrap(ByteArray(size).also { rng.nextBytes(it) })
            }

            /**
             * Picks a key from a two-pool bimodal distribution.
             * hotFraction of the time returns a key in [0, hotSize),
             * otherwise returns a key in [0, coldSize).
             */
            private fun bimodalKey(hotFraction: Double, hotSize: Long, coldSize: Long): Long {
                val rng = ThreadLocalRandom.current()
                return if (rng.nextDouble() < hotFraction) rng.nextLong(0, hotSize)
                       else rng.nextLong(0, coldSize)
            }

            /**
             * Picks a key from a three-pool graduated distribution.
             * heavyFraction → [0, heavySize); midFraction → [0, midSize); else → [0, bulkSize).
             */
            private fun trimodalKey(
                heavyFraction: Double, heavySize: Long,
                midFraction: Double, midSize: Long,
                bulkSize: Long,
            ): Long {
                val r = ThreadLocalRandom.current().nextDouble()
                return when {
                    r < heavyFraction -> ThreadLocalRandom.current().nextLong(0, heavySize)
                    r < heavyFraction + midFraction -> ThreadLocalRandom.current().nextLong(0, midSize)
                    else -> ThreadLocalRandom.current().nextLong(0, bulkSize)
                }
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val rng = ThreadLocalRandom.current()
                val bound =
                    when (pick(readThresholds)) {
                        // table1: 65.6% of reads — bimodal pool same as writes
                        0 -> {
                            val pk = bimodalKey(0.005, t1Hot, t1Cold)
                            selectTable1.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setString(1, pk.toString())
                        }
                        // table5: 12.6% of reads — bimodal pool same as writes
                        1 -> {
                            val pk = bimodalKey(0.25, t5Hot, t5Cold)
                            selectTable5.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        // table2: 11.8% of reads — bimodal pool same as writes
                        2 -> {
                            val pk = bimodalKey(0.01, t2Hot, t2Cold)
                            selectTable2.bind().setUuid(0, UUID(0L, pk))
                        }
                        // table3: 7.8% of reads — single pool
                        3 -> {
                            val pk = rng.nextLong(0, t3Pool)
                            selectTable3.bind().setUuid(0, UUID(0L, pk))
                        }
                        // table8: 0.95% of reads — single pool
                        4 -> {
                            val pk = rng.nextLong(0, t8Pool)
                            selectTable8.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        // table4: 0.92% of reads — trimodal pool same as writes
                        5 -> {
                            val pk = trimodalKey(0.01, t4Heavy, 0.09, t4Mid, t4Bulk)
                            selectTable4.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setLong(1, pk)
                        }
                        // table10: 0.29% of reads — single pool cycling CK {0,1,2}
                        6 -> {
                            val pk = rng.nextLong(0, t10Pool)
                            selectTable10.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setString(1, pk.toString())
                                .setInt(2, (pk % 3).toInt())
                        }
                        // table6: ~0.002% of reads — fixed-row pool
                        7 -> {
                            val pk = rng.nextLong(0, t6Pool)
                            selectTable6.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setString(1, pk.toString())
                                .setInt(2, (pk % Int.MAX_VALUE).toInt())
                        }
                        // table9: ~0.0008% of reads — small fixed-row pool
                        8 -> {
                            val pk = rng.nextLong(0, t9Pool)
                            selectTable9.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        // table7: ~0.0002% of reads — small always-large pool
                        else -> {
                            val pk = rng.nextLong(0, t7Pool)
                            selectTable7.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                    }
                return Operation.SelectStatement(bound)
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val rng = ThreadLocalRandom.current()
                val bound =
                    when (pick(writeThresholds)) {
                        // table1: 46.6% of writes — bimodal (hot=0.5%, cold=99.5%)
                        // P50=1 row, P99=4 rows, Max=152K — target ~110 uncompressed bytes/write
                        0 -> {
                            val pk = bimodalKey(0.005, t1Hot, t1Cold)
                            insertTable1.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setString(1, pk.toString())
                                .setUuid(2, UUID.randomUUID())    // CK — always fresh → grows row count
                                .setInt(3, rng.nextInt())
                                .setLong(4, rng.nextLong())
                                .setByteBuffer(5, blob(1, 30_000, 300_000))
                                .setString(6, "json")
                        }
                        // table2: 18.6% of writes — bimodal (hot=1%, cold=99%)
                        // P50=1 row, P75=12, P99=24, Max=73K — target ~95 uncompressed bytes/write
                        1 -> {
                            val pk = bimodalKey(0.01, t2Hot, t2Cold)
                            insertTable2.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setUuid(1, UUID.randomUUID())    // CK — always fresh
                                .setLong(2, rng.nextLong())
                                .setLong(3, rng.nextLong())
                                .setByteBuffer(4, blob(1, 6_000, 50_000))
                                .setString(5, "json")
                                .setLong(6, rng.nextLong())
                                .setByteBuffer(7, blob(1, 6_000, 50_000))
                        }
                        // table4: 18.6% of writes — trimodal (heavy=1%, mid=9%, bulk=90%)
                        // P50=4, P75=42, P95=1916, Max=35K — target ~62 uncompressed bytes/write
                        2 -> {
                            val pk = trimodalKey(0.01, t4Heavy, 0.09, t4Mid, t4Bulk)
                            insertTable4.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setLong(1, pk)
                                .setInt(2, rng.nextInt())          // CK — always fresh
                                .setLong(3, rng.nextLong())
                                .setLong(4, rng.nextLong())
                                .setByteBuffer(5, blob(1, 26, 35))
                                .setString(6, "json")
                        }
                        // table3: 13.2% of writes — single pool
                        // P50=1, P75=3, Max=35 — target ~17 uncompressed bytes/write (overhead-dominated)
                        3 -> {
                            val pk = rng.nextLong(0, t3Pool)
                            insertTable3.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setUuid(1, UUID.randomUUID())    // CK — always fresh
                                .setByteBuffer(2, blob(1, 1, 4))
                                .setString(3, "json")
                                .setLong(4, rng.nextLong())
                        }
                        // table5: 2.4% of writes — extreme bimodal (hot=25%, cold=75%)
                        // target ~18.5 uncompressed bytes/write (overhead-dominated, minimal blob)
                        4 -> {
                            val isHot = rng.nextDouble() < 0.25
                            val pk = if (isHot) rng.nextLong(0, t5Hot) else rng.nextLong(0, t5Cold)
                            insertTable5.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setInt(1, rng.nextInt())          // CK — always fresh
                                .setLong(2, rng.nextLong())
                                .setByteBuffer(3, blob(1, 1, 4))
                                .setString(4, "json")
                        }
                        // table7: 0.44% of writes — small pool, always large partitions
                        // P50=1109 rows, P75=6866 — target ~5 uncompressed bytes/write (overhead-dominated)
                        5 -> {
                            val pk = rng.nextLong(0, t7Pool)
                            insertTable7.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setLong(1, rng.nextLong())        // CK — always fresh
                                .setString(2, pk.toString())
                                .setByteBuffer(3, blob(1, 1, 4))
                                .setString(4, "json")
                                .setLong(5, rng.nextLong())
                                .setString(6, pk.toString())
                                .setLong(7, rng.nextLong())
                        }
                        // table10: 0.13% of writes — fixed 3 rows/partition, CK cycles {0,1,2}
                        // P50=P99=3 — target ~0.6 uncompressed bytes/write (overhead-dominated)
                        6 -> {
                            val pk = rng.nextLong(0, t10Pool)
                            val ck = rng.nextLong(0, 3)            // CK cycles {0,1,2} → upserts
                            insertTable10.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setString(1, pk.toString())
                                .setInt(2, (pk % 3).toInt())
                                .setLong(3, ck)
                                .setString(4, "json")
                                .setByteBuffer(5, blob(1, 1, 4))
                        }
                        // table8: 0.053% of writes — always exactly 1 row/partition
                        // P50=P99=1 row, Max=1 — target ~128 uncompressed bytes/write
                        7 -> {
                            val pk = rng.nextLong(0, t8Pool)
                            insertTable8.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setInt(1, rng.nextInt())
                                .setLong(2, rng.nextLong())
                                .setLong(3, rng.nextLong())
                                .setByteBuffer(4, blob(1, 100, 128))
                                .setString(5, "json")
                        }
                        // table6: 0.0004% of writes — fixed 3 rows/partition, CK cycles {0,1,2}
                        // P50=P99=3 — target ~0.3 uncompressed bytes/write (overhead-dominated)
                        8 -> {
                            val pk = rng.nextLong(0, t6Pool)
                            val ck = rng.nextLong(0, 3)            // CK cycles {0,1,2} → upserts
                            insertTable6.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setString(1, pk.toString())
                                .setInt(2, (pk % Int.MAX_VALUE).toInt())
                                .setInt(3, ck.toInt())             // CK field4
                                .setLong(4, ck)                    // CK field5
                                .setLong(5, rng.nextLong())
                                .setByteBuffer(6, blob(1, 1, 4))
                                .setString(7, "json")
                                .setByteBuffer(8, blob(1, 1, 4))
                                .setString(9, "json")
                        }
                        // table9: 0.00009% of writes — fixed 3 rows/partition, CK cycles {0,1,2}
                        // P50=P99=3 — target ~160 uncompressed bytes/write
                        else -> {
                            val pk = rng.nextLong(0, t9Pool)
                            val ck = rng.nextLong(0, 3)            // CK cycles {0,1,2} → upserts
                            insertTable9.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setLong(1, ck)                    // CK — cycles for fixed row count
                                .setByteBuffer(2, blob(1, 136, 160))
                                .setString(3, "json")
                                .setLong(4, rng.nextLong())
                        }
                    }
                return Operation.Mutation(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val rng = ThreadLocalRandom.current()
                val bound =
                    when (pick(writeThresholds)) {
                        0 -> {
                            val pk = bimodalKey(0.005, t1Hot, t1Cold)
                            deleteTable1.bind().setUuid(0, UUID(0L, pk)).setString(1, pk.toString())
                        }
                        1 -> {
                            val pk = bimodalKey(0.01, t2Hot, t2Cold)
                            deleteTable2.bind().setUuid(0, UUID(0L, pk))
                        }
                        2 -> {
                            val pk = trimodalKey(0.01, t4Heavy, 0.09, t4Mid, t4Bulk)
                            deleteTable4.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setLong(1, pk)
                        }
                        3 -> {
                            val pk = rng.nextLong(0, t3Pool)
                            deleteTable3.bind().setUuid(0, UUID(0L, pk))
                        }
                        4 -> {
                            val pk = bimodalKey(0.25, t5Hot, t5Cold)
                            deleteTable5.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        5 -> {
                            val pk = rng.nextLong(0, t7Pool)
                            deleteTable7.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        6 -> {
                            val pk = rng.nextLong(0, t10Pool)
                            deleteTable10.bind()
                                .setInt(0, (pk % Int.MAX_VALUE).toInt())
                                .setString(1, pk.toString())
                                .setInt(2, (pk % 3).toInt())
                        }
                        7 -> {
                            val pk = rng.nextLong(0, t8Pool)
                            deleteTable8.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                        8 -> {
                            val pk = rng.nextLong(0, t6Pool)
                            deleteTable6.bind()
                                .setUuid(0, UUID(0L, pk))
                                .setString(1, pk.toString())
                                .setInt(2, (pk % Int.MAX_VALUE).toInt())
                        }
                        else -> {
                            val pk = rng.nextLong(0, t9Pool)
                            deleteTable9.bind().setInt(0, (pk % Int.MAX_VALUE).toInt())
                        }
                    }
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getDefaultReadRate() = 0.4311

    // ── schema ────────────────────────────────────────────────────────────────
    override fun schema(): List<String> = activeTableDDL

    // 10 active tables, ordered by descending combined traffic volume
    private val activeTableDDL =
        listOf(
            // table1: composite PK=(field1 uuid,field2 text), CK=(field3 uuid ASC, field4 int ASC, field5 bigint DESC)
            """CREATE TABLE IF NOT EXISTS table1 (
            field1 uuid,
            field2 text,
            field3 uuid,
            field4 int,
            field5 bigint,
            field6 blob,
            field7 text,
            PRIMARY KEY ((field1, field2), field3, field4, field5)
        ) WITH CLUSTERING ORDER BY (field3 ASC, field4 ASC, field5 DESC)""",
            // table2: PK=(field1 uuid), CK=(field2 uuid ASC, field3 bigint ASC, field4 bigint DESC)
            """CREATE TABLE IF NOT EXISTS table2 (
            field1 uuid,
            field2 uuid,
            field3 bigint,
            field4 bigint,
            field5 blob,
            field6 text,
            field7 bigint,
            field8 blob,
            PRIMARY KEY (field1, field2, field3, field4)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 DESC)""",
            // table3: PK=(field1 uuid), CK=(field2 uuid ASC)
            """CREATE TABLE IF NOT EXISTS table3 (
            field1 uuid,
            field2 uuid,
            field3 blob,
            field4 text,
            field5 bigint,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",
            // table4: composite PK=(field1 int,field2 bigint), CK=(field3 int ASC, field4 bigint ASC, field5 bigint ASC)
            """CREATE TABLE IF NOT EXISTS table4 (
            field1 int,
            field2 bigint,
            field3 int,
            field4 bigint,
            field5 bigint,
            field6 blob,
            field7 text,
            PRIMARY KEY ((field1, field2), field3, field4, field5)
        ) WITH CLUSTERING ORDER BY (field3 ASC, field4 ASC, field5 ASC)""",
            // table5: PK=(field1 int), CK=(field2 int ASC, field3 bigint ASC)
            """CREATE TABLE IF NOT EXISTS table5 (
            field1 int,
            field2 int,
            field3 bigint,
            field4 blob,
            field5 text,
            PRIMARY KEY (field1, field2, field3)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC)""",
            // table6: composite PK=(field1 uuid,field2 text,field3 int), CK=(field4 int ASC, field5 bigint ASC)
            // Always exactly 3 rows per partition — CK cycles over {0,1,2}
            """CREATE TABLE IF NOT EXISTS table6 (
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
            // table7: PK=(field1 int), CK=(field2 bigint DESC, field3 text ASC)
            """CREATE TABLE IF NOT EXISTS table7 (
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
            // table8: PK=(field1 int), CK=(field2 int ASC, field3 bigint ASC, field4 bigint ASC)
            """CREATE TABLE IF NOT EXISTS table8 (
            field1 int,
            field2 int,
            field3 bigint,
            field4 bigint,
            field5 blob,
            field6 text,
            PRIMARY KEY (field1, field2, field3, field4)
        ) WITH CLUSTERING ORDER BY (field2 ASC, field3 ASC, field4 ASC)""",
            // table9: PK=(field1 int), CK=(field2 bigint ASC) — always exactly 3 rows, CK cycles {0,1,2}
            """CREATE TABLE IF NOT EXISTS table9 (
            field1 int,
            field2 bigint,
            field3 blob,
            field4 text,
            field5 bigint,
            PRIMARY KEY (field1, field2)
        ) WITH CLUSTERING ORDER BY (field2 ASC)""",
            // table10: composite PK=(field1 int,field2 text,field3 int), CK=(field4 bigint ASC)
            // Always exactly 3 rows per partition — CK cycles over {0,1,2}
            """CREATE TABLE IF NOT EXISTS table10 (
            field1 int,
            field2 text,
            field3 int,
            field4 bigint,
            field5 text,
            field6 blob,
            PRIMARY KEY ((field1, field2, field3), field4)
        ) WITH CLUSTERING ORDER BY (field4 ASC)""",
        )
}
