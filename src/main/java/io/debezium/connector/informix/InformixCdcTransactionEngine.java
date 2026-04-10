/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static com.informix.jdbc.stream.api.StreamRecordType.AFTER_UPDATE;
import static com.informix.jdbc.stream.api.StreamRecordType.BEFORE_UPDATE;
import static com.informix.jdbc.stream.api.StreamRecordType.COMMIT;
import static com.informix.jdbc.stream.api.StreamRecordType.DELETE;
import static com.informix.jdbc.stream.api.StreamRecordType.INSERT;
import static com.informix.jdbc.stream.api.StreamRecordType.ROLLBACK;
import static com.informix.jdbc.stream.api.StreamRecordType.TRUNCATE;

import java.nio.ByteBuffer;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.jdbc.IfxSmartBlob;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.api.StreamRecordType;
import com.informix.jdbc.stream.api.TransactionEngine;
import com.informix.jdbc.stream.cdc.CDCEngine;
import com.informix.jdbc.stream.cdc.CDCRecordBuilder;
import com.informix.jdbc.stream.cdc.records.CDCBeginTransactionRecord;
import com.informix.jdbc.stream.impl.StreamException;
import com.informix.lang.Messages;

import io.debezium.DebeziumException;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An implementation of the IfxTransactionEngine interface that takes a wider view of which operation types we are interested in.
 *
 * @author Lars M Johansson
 *
 */
public class InformixCdcTransactionEngine implements TransactionEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCdcTransactionEngine.class);
    private static final String PROCESSING_RECORD = "Processing {} record";
    private static final String MISSING_TRANSACTION_START_FOR_RECORD = "Missing transaction start for record: {}";

    protected final Builder builder;
    protected final InformixConnection connection;
    protected final ChangeEventSourceContext context;
    protected final int bufferSize;
    protected final int maxRecords;
    protected final long sequenceId;
    protected final int timeout;
    protected final List<CDCEngine.IfmxWatchedTable> watchedTables;
    protected boolean stopLoggingOnClose;
    protected boolean returnEmptyTransactions;
    protected EnumSet<StreamRecordType> operationFilters;
    protected EnumSet<StreamRecordType> transactionFilters;
    protected Map<Integer, TransactionHolder> transactionMap;
    protected Deque<StreamRecord> recordsQueue;
    protected CDCRecordBuilder recordBuilder;
    protected byte[] buffer;
    protected Map<String, TableId> tableIdByLabelId;
    protected Integer sessionId;
    protected IfxSmartBlob smartBlob;
    protected int bytesPending;

    public static Builder builder(InformixConnection connection) {
        return new Builder(connection);
    }

    protected InformixCdcTransactionEngine(Builder builder) throws SQLException {
        this.builder = builder;
        this.connection = builder.connection;
        this.context = builder.context;
        this.bufferSize = builder.bufferSize;
        this.maxRecords = builder.maxRecords;
        this.sequenceId = builder.sequenceId;
        this.timeout = builder.timeout;
        this.watchedTables = builder.watchedTables;
        this.stopLoggingOnClose = builder.stopLoggingOnClose;
        this.returnEmptyTransactions = builder.returnEmptyTransactions;
        this.operationFilters = EnumSet.of(INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, TRUNCATE);
        this.transactionFilters = EnumSet.of(COMMIT, ROLLBACK);
        this.transactionMap = new ConcurrentSkipListMap<>();
        this.recordsQueue = new ArrayDeque<>();
        this.recordBuilder = new CDCRecordBuilder(connection.connection());
        this.buffer = new byte[bufferSize];
        this.bytesPending = 0;
    }

    @Override
    public StreamRecord getRecord() throws SQLException, StreamException {
        while (context.isRunning() && recordsQueue.isEmpty() && !recordsQueue.addAll(processRecords(getRecords()))) {
            // No transactions committed but also none in flight, no more data?
            if (transactionMap.isEmpty()) {
                break;
            }
        }
        return recordsQueue.poll();
    }

    @Override
    public List<StreamRecord> getRecords() throws SQLException, StreamException {
        List<StreamRecord> records = new ArrayList<>();
        int bytesToRead = bufferSize - bytesPending;
        byte[] tmpBuffer = new byte[bytesToRead];
        int bytesRead = smartBlob.IfxLoRead(sessionId, tmpBuffer, bytesToRead);
        if (bytesRead < 0) {
            throw new StreamException("IfxLoRead returned -1, no more data?");
        }
        System.arraycopy(tmpBuffer, 0, buffer, bytesPending, bytesRead);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesPending + bytesRead);
        while (byteBuffer.remaining() > 16) {
            byteBuffer.mark();
            int headerSize = byteBuffer.getInt();
            int payloadSize = byteBuffer.getInt();
            byteBuffer.reset();

            int recordSize = headerSize + payloadSize;
            if (byteBuffer.remaining() < recordSize) {
                break;
            }

            byte[] recordBytes = new byte[recordSize];
            byteBuffer.get(recordBytes);
            StreamRecord record = recordBuilder.buildRecord(recordBytes);
            records.add(record);
        }

        bytesPending = byteBuffer.remaining();
        System.arraycopy(buffer, byteBuffer.position(), buffer, 0, bytesPending);

        return records;
    }

    private List<StreamRecord> processRecords(List<StreamRecord> records) throws SQLException, StreamException {
        return records.stream().map(this::processRecord).filter(Objects::nonNull).toList();
    }

    private StreamRecord processRecord(StreamRecord streamRecord) {
        TransactionHolder holder = transactionMap.get(streamRecord.getTransactionId());
        if (holder != null) {
            LOGGER.debug("Processing [{}] record for transaction id: {}", streamRecord.getType(), streamRecord.getTransactionId());
        }
        switch (streamRecord.getType()) {
            case BEGIN -> {
                holder = new TransactionHolder();
                holder.beginRecord = (CDCBeginTransactionRecord) streamRecord;
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
    public InformixStreamTransactionRecord getTransaction() throws SQLException, StreamException {
        StreamRecord streamRecord;
        while ((streamRecord = getRecord()) != null && !(streamRecord instanceof InformixStreamTransactionRecord)) {
            LOGGER.debug("Discard non-transaction record: {}", streamRecord);
        }
        return (InformixStreamTransactionRecord) streamRecord;
    }

    @Override
    public InformixCdcTransactionEngine setOperationFilters(StreamRecordType... recordTypes) {
        operationFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine setTransactionFilters(StreamRecordType... recordTypes) {
        transactionFilters = EnumSet.copyOf(Set.of(recordTypes));
        return this;
    }

    @Override
    public InformixCdcTransactionEngine returnEmptyTransactions(boolean returnEmptyTransactions) {
        this.returnEmptyTransactions = returnEmptyTransactions;
        return this;
    }

    @Override
    public void init() throws SQLException, StreamException {
        openSession();

        this.smartBlob = new IfxSmartBlob(this.connection.connection());

        tableIdByLabelId = new ConcurrentSkipListMap<>();
        for (CDCEngine.IfmxWatchedTable table : this.watchedTables) {
            watchTable(table);
            tableIdByLabelId.put(String.valueOf(table.getLabel()), new TableId(table.getDatabaseName(), table.getNamespace(), table.getTableName()));
        }

        activateSession();
    }

    private void openSession() throws StreamException {
        try {
            String serverName = this.connection.queryAndMap("select env_value from sysmaster:sysenv where env_name = 'INFORMIXSERVER'",
                    rs -> rs.next() ? rs.getString(1).trim() : "");
            LOGGER.debug("Server name detected: {}", serverName);
            this.sessionId = this.connection.prepareQueryAndMap("execute function informix.cdc_opensess(?,?,?,?,?,?)", cstmt -> {
                cstmt.setString(1, serverName);
                cstmt.setInt(2, 0);
                cstmt.setInt(3, this.timeout);
                cstmt.setInt(4, this.maxRecords);
                cstmt.setInt(5, 1);
                cstmt.setInt(6, 1);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (this.sessionId < 0) {
                throw new StreamException("Unable to create CDC session: {}".formatted(Messages.getMessage(this.sessionId)), this.sessionId);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to create CDC session ", e);
        }
    }

    private void watchTable(CDCEngine.IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Starting watch on table [{}]", table);
        setFullRowLogging(table.getDesciptorString(), true);
        startCapture(table);
    }

    private void setFullRowLogging(String tableName, boolean enable) throws StreamException {
        LOGGER.debug("Setting full row logging on [{}] to '{}'", tableName, enable);
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_set_fullrowlogging(?,?)", cstmt -> {
                cstmt.setString(1, tableName);
                cstmt.setInt(2, enable ? 1 : 0);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new StreamException("Unable to set full row logging: {}".formatted(Messages.getMessage(resultCode)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to set full row logging ", e);
        }
    }

    private void startCapture(CDCEngine.IfmxWatchedTable table) throws StreamException {
        // if (table.getColumnDescriptorString().equals("*")) {}
        LOGGER.debug("Starting capture on [{}]", table);
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_startcapture(?,?,?,?,?)", cstmt -> {
                cstmt.setInt(1, this.sessionId);
                cstmt.setLong(2, 0L);
                cstmt.setString(3, table.getDesciptorString());
                cstmt.setString(4, table.getColumnDescriptorString());
                cstmt.setInt(5, table.getLabel());
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new StreamException("Unable to start cdc capture: {}".formatted(Messages.getMessage(83723)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to start cdc capture ", e);
        }
    }

    private void activateSession() throws StreamException {
        LOGGER.debug("Activating CDC session");
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_activatesess(?,?)", cstmt -> {
                cstmt.setInt(1, this.sessionId);
                cstmt.setLong(2, this.sequenceId);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new StreamException("Unable to activate session: {}".formatted(Messages.getMessage(resultCode), resultCode));
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to activate session ", e);
        }
    }

    private void closeSession() throws StreamException {
        LOGGER.debug("Closing CDC session");
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_closesess(?)", cstmt -> {
                cstmt.setInt(1, this.sessionId);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new StreamException("Unable to close session: {}".formatted(Messages.getMessage(resultCode), resultCode));
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to close session ", e);
        }
    }

    private void endCapture(CDCEngine.IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Ending capture on [{}]", table);
        Integer resultCode = null;
        try {
            resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_endcapture(?,0,?)", cstmt -> {
                cstmt.setInt(1, this.sessionId);
                cstmt.setString(2, table.getDesciptorString());
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new StreamException("Unable to end cdc capture: {}".formatted(Messages.getMessage(83723)), resultCode);
            }
        }
        catch (SQLException e) {
            throw new StreamException("Unable to end cdc capture ", e);
        }
    }

    private void unwatchTable(CDCEngine.IfmxWatchedTable table) throws StreamException {
        LOGGER.debug("Ending watch on table [{}]", table);
        endCapture(table);
        if (this.stopLoggingOnClose) {
            setFullRowLogging(table.getDesciptorString(), false);
        }
    }

    @Override
    public void close() {
        LOGGER.debug("Closing down CDC engine");
        try {
            for (CDCEngine.IfmxWatchedTable capturedTable : this.watchedTables) {
                unwatchTable(capturedTable);
            }
            closeSession();
        }
        catch (StreamException e) {
            throw new DebeziumException("Exception caught when closing CDC engine ", e);
        }
    }

    public OptionalLong getLowestBeginSequence() {
        return transactionMap.values().stream().mapToLong(t -> t.beginRecord.getSequenceId()).min();
    }

    public Map<String, TableId> getTableIdByLabelId() {
        return tableIdByLabelId;
    }

    protected static class TransactionHolder {
        final List<StreamRecord> records = new ArrayList<>();
        CDCBeginTransactionRecord beginRecord;
        StreamRecord closingRecord;
    }

    public Builder getBuilder() {
        return builder;
    }

    public static class Builder {

        private final InformixConnection connection;
        private ChangeEventSourceContext context;
        private int bufferSize;
        private int maxRecords;
        private long sequenceId;
        private int timeout;
        private final List<CDCEngine.IfmxWatchedTable> watchedTables = new ArrayList();
        private boolean stopLoggingOnClose = true;
        private boolean returnEmptyTransactions = false;

        protected Builder(InformixConnection connection) {
            this.connection = connection;
        }

        public InformixConnection getConnection() {
            return connection;
        }

        public Builder context(ChangeEventSourceContext context) {
            this.context = context;
            return this;
        }

        public Builder buffer(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public Builder maxRecords(int maxRecords) {
            this.maxRecords = maxRecords;
            return this;
        }

        public int getMaxRecords() {
            return maxRecords;
        }

        public Builder sequenceId(long position) {
            this.sequenceId = position;
            return this;
        }

        public long getSequenceId() {
            return sequenceId;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public Builder watchTable(String canonicalTableName, String... columns) {
            return this.watchTable(IfmxTableDescriptor.parse(canonicalTableName), columns);
        }

        public Builder watchTable(IfmxTableDescriptor desc, String... columns) {
            return this.watchTable((new CDCEngine.IfmxWatchedTable(desc)).columns(columns));
        }

        public Builder watchTable(CDCEngine.IfmxWatchedTable table) {
            this.watchedTables.add(table);
            return this;
        }

        public List<CDCEngine.IfmxWatchedTable> getWatchedTables() {
            return watchedTables;
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            this.stopLoggingOnClose = stopOnClose;
            return this;
        }

        public Builder returnEmptyTransactions(boolean returnEmptyTransactions) {
            this.returnEmptyTransactions = returnEmptyTransactions;
            return this;
        }

        public InformixCdcTransactionEngine build() throws SQLException {
            return new InformixCdcTransactionEngine(this);
        }
    }
}
