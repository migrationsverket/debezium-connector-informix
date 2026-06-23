/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.ConditionalFailExtension;

/**
 * Integration test for the Debezium Informix connector.
 *
 */
@ExtendWith(ConditionalFailExtension.class)
public class InformixLargeTranactionIT extends AbstractAsyncEngineConnectorTest {

    private InformixConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute(
                "DROP TABLE IF EXISTS tablea",
                "DROP TABLE IF EXISTS tableb",
                "DROP TABLE IF EXISTS tablec",
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "CREATE TABLE tablec (id int not null, colc varchar(30), primary key (id))");
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.disable();
    }

    @AfterEach
    public void after() throws SQLException {
        /*
         * Since all DDL operations are forbidden during Informix CDC,
         * we have to ensure the connector is properly shut down before dropping tables.
         */
        stopConnector();
        waitForConnectorShutdown(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);
        assertConnectorNotRunning();
        if (connection != null) {
            connection.rollback()
                    .execute(
                            "DROP TABLE tablea",
                            "DROP TABLE tableb",
                            "DROP TABLE tablec")
                    .close();
        }
    }

    private void testLargeTransaction(Configuration config) throws InterruptedException, SQLException {
        final int RECORDS_PER_TABLE = 10_000;
        final int ID_START = 1;

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to start
        waitForStreamingRunning(TestHelper.TEST_CONNECTOR, TestHelper.TEST_DATABASE);

        connection.setAutoCommit(false);
        try (PreparedStatement ps1 = connection.connection().prepareStatement("INSERT INTO tablea VALUES (?,?)");
             PreparedStatement ps2 = connection.connection().prepareStatement("INSERT INTO tableb VALUES (?,?)");
             PreparedStatement ps3 = connection.connection().prepareStatement("INSERT INTO tablec VALUES (?,?)")) {
            for (int i = 1; i <= RECORDS_PER_TABLE; i++) {
                ps1.setInt(1, i);
                ps1.setInt(2, 'a');
                ps1.addBatch();

                ps2.setInt(1, i);
                ps2.setInt(2, 'b');
                ps2.addBatch();

                ps3.setInt(1, i);
                ps3.setInt(2, 'c');
                ps3.addBatch();
            }
            ps1.executeBatch();
            connection.commit();
            ps2.executeBatch();
            connection.commit();
            ps3.executeBatch();
            connection.commit();
        }

        waitForAvailableRecords();

        final SourceRecords records = consumeAvailableRecordsByTopic();
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.informix.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.informix.tableb");
        final List<SourceRecord> tableC = records.recordsForTopic("testdb.informix.tablec");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertThat(tableC).hasSize(RECORDS_PER_TABLE);
        assertNoRecordsToConsume();
    }

    private static Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 46_368)
                .with(InformixConnectorConfig.CDC_TIMEOUT, 0)
                .with(InformixConnectorConfig.CDC_BUFFERSIZE, 0x100_0000)
                .with(InformixConnectorConfig.CDC_MAX_RECORDS, 0x100)
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, "testdb.informix.tableb,testdb.informix.tableb,testdb.informix.tablec");
    }

    @Test
    public void testLargeTransactionWithDefault() throws Exception {
        final Configuration config = getConfig().build();

        testLargeTransaction(config);
    }

    @Test
    public void testLargeTransactionWithCaffeine() throws Exception {
        final Configuration config = getConfig()
                .with(InformixConnectorConfig.JCACHE_PROVIDER_CLASSNAME, "com.github.benmanes.caffeine.jcache.spi.CaffeineCachingProvider")
                .with(InformixConnectorConfig.JCACHE_URI, "caffeine.conf")
                .with(InformixConnectorConfig.TRANSACTION_CACHE_NAME, "caffeine-transaction-cache")
                .build();

        testLargeTransaction(config);
    }

    @Test
    public void testLargeTransactionWithHazelcast() throws Exception {
        final Configuration config = getConfig()
                .with(InformixConnectorConfig.JCACHE_PROVIDER_CLASSNAME, "com.hazelcast.cache.HazelcastMemberCachingProvider")
                .with(InformixConnectorConfig.JCACHE_URI, "hazelcast.yaml")
                .with(InformixConnectorConfig.TRANSACTION_CACHE_NAME, "hazelcast-transaction-cache")
                .build();

        testLargeTransaction(config);
    }

    @Test
    public void testLargeTransactionWithEhCache() throws Exception {
        final Configuration config = getConfig()
                .with(InformixConnectorConfig.JCACHE_PROVIDER_CLASSNAME, "org.ehcache.jsr107.EhcacheCachingProvider")
                .with(InformixConnectorConfig.JCACHE_URI, "ehcache.xml")
                .with(InformixConnectorConfig.TRANSACTION_CACHE_NAME, "terracotta-transaction-cache")
                .build();

        testLargeTransaction(config);
    }

}
