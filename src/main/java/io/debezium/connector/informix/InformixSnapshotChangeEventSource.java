/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.informix.InformixConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.informix.InformixOffsetContext.Loader;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class InformixSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixSnapshotChangeEventSource.class);

    private final InformixConnectorConfig connectorConfig;
    private final InformixConnection jdbcConnection;

    public InformixSnapshotChangeEventSource(InformixConnectorConfig connectorConfig,
                                             MainConnectionProvidingConnectionFactory<InformixConnection> connectionFactory,
                                             InformixDatabaseSchema schema, EventDispatcher<InformixPartition, TableId> dispatcher,
                                             Clock clock, SnapshotProgressListener<InformixPartition> snapshotProgressListener,
                                             NotificationService<InformixPartition, InformixOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
    }

    @Override
    public SnapshottingTask getSnapshottingTask(InformixPartition partition, InformixOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().identifier(), Map.Entry::getValue));

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<InformixPartition, InformixOffsetContext> prepare(InformixPartition partition) throws Exception {
        return new InformixSnapshotContext(partition, jdbcConnection.getRealDatabaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) throws Exception {
        ((InformixSnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> ctx) throws Exception {
        return jdbcConnection.readAllTableNames(new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_COMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.EXCLUSIVE
                || connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            ((InformixSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("informix_schema_snapshot");

            LOGGER.info("Executing schema locking");
            try (Statement statement = jdbcConnection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    LOGGER.info("Locking table {}", tableId);
                    String query = String.format("LOCK TABLE %s.%s IN %s MODE", tableId.schema(), tableId.table(),
                            connectorConfig.getSnapshotIsolationMode().equals(SnapshotIsolationMode.EXCLUSIVE) ? "EXCLUSIVE" : "SHARE");
                    statement.execute(query);
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; read_committed mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((InformixSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> ctx, InformixOffsetContext previousOffset) {
        InformixOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null) {
                offset = previousOffset;
            }
            else {
                offset = new InformixOffsetContext(
                connectorConfig,
                        TxLogPosition.valueOf(jdbcConnection.getMaxLsn()),
                false,
                false);
    }
            ctx.offset = offset;
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext,
                                      InformixOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables,
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", schema);

            TableFilter tableFilter = snapshottingTask.isBlocking()
                    ? TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void aborted(SnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    private void close(SnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((InformixSnapshotContext) snapshotContext).isolationLevelBeforeStart);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid Informix query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext, TableId tableId,
                                                 List<String> columns) {
        String snapshotSelectColumns = !columns.isEmpty() ? Strings.join(", ", columns) : "*";
        return Optional.of(String.format("SELECT %s FROM %s.%s", snapshotSelectColumns, tableId.schema(), tableId.table()));
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class InformixSnapshotContext extends RelationalSnapshotContext<InformixPartition, InformixOffsetContext> {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        InformixSnapshotContext(InformixPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }

    @Override
    protected InformixOffsetContext copyOffset(RelationalSnapshotContext<InformixPartition, InformixOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}