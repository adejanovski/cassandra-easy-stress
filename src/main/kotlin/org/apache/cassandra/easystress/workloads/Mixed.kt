package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext

/**
 * Multi-table mixed workload modelling a real Cassandra keyspace traffic shape.
 * 13 active tables are included in the read/write dispatch; 14 zero-traffic tables
 * have their schema emitted so the keyspace structure mirrors production.
 * Per-table read/write ratios and blob sizes are derived from cfstats.
 */
class Mixed : IStressWorkload {

    // ── stubs (filled in Task 2) ──────────────────────────────────────────────
    override fun prepare(session: CqlSession) = Unit

    override fun getRunner(context: StressContext): IStressRunner =
        throw NotImplementedError("Implemented in Task 2")

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
