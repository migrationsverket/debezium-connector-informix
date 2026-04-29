/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.informix.jdbc.stream.api.RecordStream;
import com.informix.jdbc.stream.api.StreamListener;
import com.informix.jdbc.stream.api.StreamRecord;
import com.informix.jdbc.stream.impl.StreamException;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

public class DbzRecordStreamRunner implements RecordStream {

    protected final ChangeEventSourceContext context;
    protected final DbzTransactionEngine engine;
    protected final List<StreamListener> listeners = new CopyOnWriteArrayList<>();
    protected final List<StreamException> exceptions = new ArrayList<>();
    protected final boolean stopOnError;

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine) {
        this(context, engine, true);
    }

    public DbzRecordStreamRunner(ChangeEventSourceContext context, DbzTransactionEngine engine, boolean stopOnError) {
        this.context = context;
        this.engine = engine;
        this.stopOnError = stopOnError;
    }

    @Override
    public void run() {
        try {
            engine.init();
        }
        catch (SQLException e) {
            exceptions.add(new StreamException("SQL exception caught during engine initialization ", e));
            return;
        }
        catch (StreamException e) {
            exceptions.add(e);
            return;
        }

        while (context.isRunning() && (!stopOnError || exceptions.isEmpty())) {
            try {
                for (StreamRecord sr : engine.getRecords()) {
                    StreamRecord pr = engine.processRecord(sr);
                    if (pr != null) {
                        for (StreamListener listener : listeners) {
                            try {
                                listener.accept(pr);
                            }
                            catch (SQLException e) {
                                exceptions.add(new StreamException("SQL exception occurred in listener [%s] while processing record [%s]".formatted(listener, pr), e));
                            }
                            catch (StreamException e) {
                                exceptions.add(e);
                            }
                        }
                    }
                }
            }
            catch (SQLException e) {
                exceptions.add(new StreamException("SQL exception caught processing records ", e));
            }
            catch (StreamException e) {
                exceptions.add(e);
            }
        }
    }

    @Override
    public RecordStream addListener(StreamListener streamListener) {
        listeners.add(streamListener);
        return this;
    }

    @Override
    public List<StreamException> getExceptions() {
        return exceptions;
    }

    @Override
    public void close() throws Exception {
        engine.close();
    }
}
