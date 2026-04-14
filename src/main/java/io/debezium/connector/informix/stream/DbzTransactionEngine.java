/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import static com.informix.stream.api.IfmxStreamRecordType.AFTER_UPDATE;
import static com.informix.stream.api.IfmxStreamRecordType.BEFORE_UPDATE;
import static com.informix.stream.api.IfmxStreamRecordType.COMMIT;
import static com.informix.stream.api.IfmxStreamRecordType.DELETE;
import static com.informix.stream.api.IfmxStreamRecordType.INSERT;
import static com.informix.stream.api.IfmxStreamRecordType.ROLLBACK;
import static com.informix.stream.api.IfmxStreamRecordType.TRUNCATE;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.api.IfmxStreamRecordType;
import com.informix.stream.api.IfxTransactionEngine;
import com.informix.stream.cdc.IfxCDCEngine.IfmxWatchedTable;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.connector.informix.InformixConnection;
import io.debezium.connector.informix.InformixStreamTransactionRecord;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An implementation of the IfxTransactionEngine interface that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 *
 */
public class DbzTransactionEngine implements IfxTransactionEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbzTransactionEngine.class);
    private static final String PROCESSING_RECORD = "Processing {} record";
    private static final String MISSING_TRANSACTION_START_FOR_RECORD = "Missing transaction start for record: {}";

    protected final Builder builder;
    protected final DbzCDCEngine engine;
    protected final ChangeEventSourceContext context;
    protected boolean returnEmptyTransactions;
    protected EnumSet<IfmxStreamRecordType> operationFilters;
    protected EnumSet<IfmxStreamRecordType> transactionFilters;
    protected final Map<Integer, TransactionHolder> transactionMap;
    protected final Deque<IfmxStreamRecord> recordsQueue;
    protected Map<String, TableId> tableIdByLabelId;

    public static Builder builder(InformixConnection connection) {
        return new Builder(connection);
    }

    protected DbzTransactionEngine(Builder builder) {
        this.builder = builder;
        this.engine = builder.engine;
        this.context = builder.context;
        this.returnEmptyTransactions = builder.returnEmptyTransactions;
        this.operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
        this.transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
        this.transactionMap = new ConcurrentSkipListMap<>();
        this.recordsQueue = new ArrayDeque<>();
    }

    @Override
    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        while (context.isRunning() && recordsQueue.isEmpty() && !recordsQueue.addAll(processRecords())) {
            // No transactions committed but also none in flight, no more data?
            if (transactionMap.isEmpty()) {
                break;
            }
        }
        return recordsQueue.poll();
    }

    @Override
    public List<IfmxStreamRecord> getRecords() throws SQLException, IfxStreamException {
        return engine.getRecords();
    }

    List<IfmxStreamRecord> processRecords() throws SQLException, IfxStreamException {
        return this.getRecords().stream().map(this::processRecord).filter(Objects::nonNull).toList();
    }

    IfmxStreamRecord processRecord(IfmxStreamRecord streamRecord) {
        TransactionHolder holder = transactionMap.get(streamRecord.getTransactionId());
        if (holder != null) {
            LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
        }
        switch (streamRecord.getType()) {
            case BEGIN -> {
                holder = new TransactionHolder();
                holder.beginRecord = (IfxCDCBeginTransactionRecord) streamRecord;
                transactionMap.put(streamRecord.getTransactionId(), holder);
                LOGGER.debug("Watching transaction id: {}", streamRecord.getTransactionId());
            }
            case INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE -> {
                if (holder == null) {
                    LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                    break;
                }
                LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                holder.records.add(streamRecord);
            }
            case DISCARD -> {
                if (holder == null) {
                    LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                    break;
                }
                LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                long sequenceId = streamRecord.getSequenceId();

                if (holder.records.removeIf(r -> r.getSequenceId() >= sequenceId)) {
                    LOGGER.debug("Discarding records with sequence >={}", sequenceId);
                }
            }
            case COMMIT, ROLLBACK -> {
                if (holder == null) {
                    LOGGER.warn(MISSING_TRANSACTION_START_FOR_RECORD, streamRecord);
                    break;
                }
                LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                holder.closingRecord = streamRecord;
            }
            case METADATA, TIMEOUT, ERROR -> {
                LOGGER.debug(PROCESSING_RECORD, streamRecord.getType());
                if (holder == null) {
                    return streamRecord;
                }
                holder.records.add(streamRecord);
            }
            default -> LOGGER.warn("Unknown operation for record: {}", streamRecord);
        }
        if (holder != null && holder.closingRecord != null) {
            transactionMap.remove(streamRecord.getTransactionId());
            if (!holder.records.isEmpty() || returnEmptyTransactions) {
                return new InformixStreamTransactionRecord(holder.beginRecord, holder.closingRecord, holder.records);
            }
        }

        return null;
    }

    @Override
    public InformixStreamTransactionRecord getTransaction() throws SQLException, IfxStreamException {
        IfmxStreamRecord streamRecord;
        while ((streamRecord = getRecord()) != null && !(streamRecord instanceof InformixStreamTransactionRecord)) {
            LOGGER.debug("Discard non-transaction record: {}", streamRecord);
        }
        return (InformixStreamTransactionRecord) streamRecord;
    }

    @Override
    public DbzTransactionEngine setOperationFilters(IfmxStreamRecordType... recordTypes) {
        operationFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public DbzTransactionEngine setTransactionFilters(IfmxStreamRecordType... recordTypes) {
        transactionFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public DbzTransactionEngine returnEmptyTransactions(boolean returnEmptyTransactions) {
        this.returnEmptyTransactions = returnEmptyTransactions;
        return this;
    }

    @Override
    public void init() throws SQLException, IfxStreamException {
        engine.init();

        /*
         * Build Map of Label_id to TableId.
         */
        tableIdByLabelId = builder.getWatchedTables().stream()
                .collect(Collectors.toUnmodifiableMap(
                        t -> String.valueOf(t.getLabel()),
                        t -> TableId.parse("%s.%s.%s".formatted(t.getDatabaseName(), t.getNamespace(), t.getTableName()))));
    }

    @Override
    public void close() throws IfxStreamException {
        engine.close();
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionMap.values().stream().mapToLong(t -> t.beginRecord.getSequenceId()).min();
    }

    public Map<String, TableId> getTableIdByLabelId() {
        return tableIdByLabelId;
    }

    protected static class TransactionHolder {
        final List<IfmxStreamRecord> records = new ArrayList<>();
        IfxCDCBeginTransactionRecord beginRecord;
        IfmxStreamRecord closingRecord;
    }

    public Builder getBuilder() {
        return builder;
    }

    public static class Builder {

        private final DbzCDCEngine.Builder builder;
        private DbzCDCEngine engine;
        private ChangeEventSourceContext context;
        private boolean returnEmptyTransactions = false;

        protected Builder(InformixConnection connection) {
            this.builder = DbzCDCEngine.builder(connection);
        }

        public InformixConnection getConnection() {
            return builder.getConnection();
        }

        public Builder context(ChangeEventSourceContext context) {
            this.context = context;
            return this;
        }

        public Builder buffer(int bufferSize) {
            builder.buffer(bufferSize);
            return this;
        }

        public int getBufferSize() {
            return builder.getBufferSize();
        }

        public Builder maxRecords(int maxRecords) {
            builder.maxRecords(maxRecords);
            return this;
        }

        public int getMaxRecords() {
            return builder.getMaxRecords();
        }

        public Builder sequenceId(long position) {
            builder.sequenceId(position);
            return this;
        }

        public long getSequenceId() {
            return builder.getSequenceId();
        }

        public Builder timeout(int timeout) {
            builder.timeout(timeout);
            return this;
        }

        public int getTimeout() {
            return builder.getTimeout();
        }

        public Builder watchTable(String canonicalTableName, String... columns) {
            builder.watchTable(canonicalTableName, columns);
            return this;
        }

        public Builder watchTable(IfmxTableDescriptor desc, String... columns) {
            builder.watchTable(desc, columns);
            return this;
        }

        public Builder watchTable(IfmxWatchedTable table) {
            builder.watchTable(table);
            return this;
        }

        public List<IfmxWatchedTable> getWatchedTables() {
            return builder.getWatchedTables();
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            builder.stopLoggingOnClose(stopOnClose);
            return this;
        }

        public Builder returnEmptyTransactions(boolean returnEmptyTransactions) {
            this.returnEmptyTransactions = returnEmptyTransactions;
            return this;
        }

        public DbzTransactionEngine build() throws SQLException {
            engine = builder.build();
            return new DbzTransactionEngine(this);
        }
    }
}
