/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.jdbc.IfxSmartBlob;
import com.informix.stream.api.IfmxStreamEngine;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.IfxCDCEngine.IfmxWatchedTable;
import com.informix.stream.cdc.IfxCDCRecordBuilder;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.DebeziumException;
import io.debezium.connector.informix.InformixConnection;

public class DbzCDCEngine implements IfmxStreamEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbzCDCEngine.class);

    protected final Builder builder;
    protected final InformixConnection connection;
    protected final int bufferSize;
    protected final int maxRecords;
    protected final long position;
    protected final int timeout;
    protected final List<IfmxWatchedTable> watchedTables;
    protected final boolean stopLoggingOnClose;
    protected final IfxCDCRecordBuilder recordBuilder;
    protected final byte[] buffer;
    protected int sessionId;
    protected IfxSmartBlob smartBlob;
    protected int bytesPending;

    public static Builder builder(InformixConnection connection) {
        return new Builder(connection);
    }

    protected DbzCDCEngine(Builder builder) throws SQLException {
        this.builder = builder;
        this.connection = builder.connection;
        this.bufferSize = builder.bufferSize;
        this.maxRecords = builder.maxRecords;
        this.position = builder.position;
        this.timeout = builder.timeout;
        this.watchedTables = builder.watchedTables;
        this.stopLoggingOnClose = builder.stopLoggingOnClose;
        this.recordBuilder = new IfxCDCRecordBuilder(connection.connection());
        this.buffer = new byte[bufferSize];
        this.bytesPending = 0;
    }

    @Override
    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        if (bytesPending < 16) {
            int bytesToRead = bufferSize - bytesPending;
            byte[] tmpBuffer = new byte[bytesToRead];
            int bytesRead = smartBlob.IfxLoRead(sessionId, tmpBuffer, bytesToRead);
            if (bytesRead < 0) {
                throw new IfxStreamException("IfxLoRead returned -1, no more data?");
            }
            System.arraycopy(tmpBuffer, 0, buffer, bytesPending, bytesRead);
            bytesPending += bytesRead;
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesPending);

        byteBuffer.mark();
        int headerSize = byteBuffer.getInt();
        int payloadSize = byteBuffer.getInt();
        byteBuffer.reset();

        int recordSize = headerSize + payloadSize;
        if (byteBuffer.remaining() < recordSize) {
            return null; // Throw exception?
        }

        byte[] recordBytes = new byte[recordSize];
        byteBuffer.get(recordBytes);
        IfmxStreamRecord record = recordBuilder.buildRecord(recordBytes);

        bytesPending = byteBuffer.remaining();
        System.arraycopy(buffer, byteBuffer.position(), buffer, 0, bytesPending);

        return record;
    }

    @Override
    public List<IfmxStreamRecord> getRecords() throws SQLException, IfxStreamException {
        List<IfmxStreamRecord> records = new ArrayList<>();
        int bytesToRead = bufferSize - bytesPending;
        byte[] tmpBuffer = new byte[bytesToRead];
        int bytesRead = smartBlob.IfxLoRead(sessionId, tmpBuffer, bytesToRead);
        if (bytesRead < 0) {
            throw new IfxStreamException("IfxLoRead returned -1, no more data?");
        }

        System.arraycopy(tmpBuffer, 0, buffer, bytesPending, bytesRead);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesPending + bytesRead);

        while (byteBuffer.remaining() >= 16) {
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
            IfmxStreamRecord record = recordBuilder.buildRecord(recordBytes);
            records.add(record);
        }

        bytesPending = byteBuffer.remaining();
        System.arraycopy(buffer, byteBuffer.position(), buffer, 0, bytesPending);

        return records;
    }

    @Override
    public void init() throws SQLException, IfxStreamException {
        openSession();

        this.smartBlob = new IfxSmartBlob(this.connection.connection());

        for (IfmxWatchedTable table : this.watchedTables) {
            watchTable(table);
        }

        activateSession();
    }

    private void openSession() throws IfxStreamException {
        try {
            String serverName = this.connection.queryAndMap("select env_value from sysmaster:sysenv where env_name = 'INFORMIXSERVER'",
                    rs -> rs.next() ? rs.getString(1).trim() : "");
            LOGGER.debug("Server name detected: {}", serverName);
            this.sessionId = this.connection.prepareQueryAndMap("execute function informix.cdc_opensess(?,?,?,?,?,?)", ps -> {
                ps.setString(1, serverName);
                ps.setInt(2, 0);
                ps.setInt(3, this.timeout);
                ps.setInt(4, this.maxRecords);
                ps.setInt(5, 1);
                ps.setInt(6, 1);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (this.sessionId < 0) {
                throw new IfxStreamException("Unable to create CDC session. Error code: " + this.sessionId);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to create CDC session ", e);
        }
    }

    private void closeSession() throws IfxStreamException {
        LOGGER.debug("Closing CDC session");
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_closesess(?)",
                    ps -> ps.setInt(1, this.sessionId),
                    rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new IfxStreamException("Unable to close session. Error code: " + resultCode);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to close session ", e);
        }
    }

    private void watchTable(IfmxWatchedTable table) throws IfxStreamException {
        LOGGER.debug("Starting watch on table [{}]", table);
        setFullRowLogging(table.getDesciptorString(), true);
        startCapture(table);
    }

    private void unwatchTable(IfmxWatchedTable table) throws IfxStreamException {
        LOGGER.debug("Ending watch on table [{}]", table);
        endCapture(table);
        if (this.stopLoggingOnClose) {
            setFullRowLogging(table.getDesciptorString(), false);
        }
    }

    private void setFullRowLogging(String tableName, boolean enable) throws IfxStreamException {
        LOGGER.debug("Setting full row logging on [{}] to '{}'", tableName, enable);
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_set_fullrowlogging(?,?)", ps -> {
                ps.setString(1, tableName);
                ps.setInt(2, enable ? 1 : 0);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new IfxStreamException("Unable to set full row logging. Error code: " + resultCode);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to set full row logging ", e);
        }
    }

    private void startCapture(IfmxWatchedTable table) throws IfxStreamException {
        try {
            if (table.getColumnDescriptorString().equals("*")) {
                LOGGER.debug("Starting column lookup for [{}]", table.getDesciptorString());

                String[] columns = this.connection.queryAndMap("SELECT FIRST 1 * FROM " + table.getDesciptorString(), rs -> {
                    ResultSetMetaData md = rs.getMetaData();
                    String[] c = new String[md.getColumnCount()];
                    for (int i = 1; i <= c.length; i++) {
                        c[i - 1] = md.getColumnName(i).trim();
                    }
                    return c;
                });
                LOGGER.debug("Dynamically adding to table [{}] columns: {}", table.getDesciptorString(), columns);
                table.columns(columns);
            }

            LOGGER.debug("Starting capture on [{}]", table);
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_startcapture(?,?,?,?,?)", ps -> {
                ps.setInt(1, this.sessionId);
                ps.setLong(2, 0L);
                ps.setString(3, table.getDesciptorString());
                ps.setString(4, table.getColumnDescriptorString());
                ps.setInt(5, table.getLabel());
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new IfxStreamException("Unable to start cdc capture. Error code: " + resultCode);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to start cdc capture ", e);
        }
    }

    private void endCapture(IfmxWatchedTable table) throws IfxStreamException {
        LOGGER.debug("Ending capture on [{}]", table);
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_endcapture(?,0,?)", ps -> {
                ps.setInt(1, this.sessionId);
                ps.setString(2, table.getDesciptorString());
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new IfxStreamException("Unable to end cdc capture. Error code: " + resultCode);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to end cdc capture ", e);
        }
    }

    private void activateSession() throws IfxStreamException {
        LOGGER.debug("Activating CDC session");
        try {
            Integer resultCode = this.connection.prepareQueryAndMap("execute function informix.cdc_activatesess(?,?)", ps -> {
                ps.setInt(1, this.sessionId);
                ps.setLong(2, this.position);
            }, rs -> rs.next() ? rs.getInt(1) : -1);
            if (resultCode != 0) {
                throw new IfxStreamException("Unable to activate session. Error code: " + resultCode);
            }
        }
        catch (SQLException e) {
            throw new IfxStreamException("Unable to activate session ", e);
        }
    }

    @Override
    public void close() {
        LOGGER.debug("Closing down CDC engine");
        try {
            for (IfmxWatchedTable capturedTable : this.watchedTables) {
                unwatchTable(capturedTable);
            }
            closeSession();
        }
        catch (IfxStreamException e) {
            throw new DebeziumException("Exception caught when closing CDC engine ", e);
        }
    }

    public Builder getBuilder() {
        return builder;
    }

    public static class Builder {

        private final InformixConnection connection;
        private int bufferSize;
        private int maxRecords;
        private long position;
        private int timeout;
        private final List<IfmxWatchedTable> watchedTables = new ArrayList<>();
        private boolean stopLoggingOnClose = true;

        protected Builder(InformixConnection connection) {
            this.connection = connection;
        }

        public InformixConnection getConnection() {
            return connection;
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
            this.position = position;
            return this;
        }

        public long getSequenceId() {
            return position;
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
            return this.watchTable((new IfmxWatchedTable(desc)).columns(columns));
        }

        public Builder watchTable(IfmxWatchedTable table) {
            this.watchedTables.add(table);
            return this;
        }

        public List<IfmxWatchedTable> getWatchedTables() {
            return watchedTables;
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            this.stopLoggingOnClose = stopOnClose;
            return this;
        }

        public DbzCDCEngine build() throws SQLException {
            return new DbzCDCEngine(this);
        }
    }
}
