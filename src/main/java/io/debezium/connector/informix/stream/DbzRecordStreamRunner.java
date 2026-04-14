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

import com.informix.stream.api.IfmxStreamListener;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.api.IfxStream;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

public class DbzRecordStreamRunner implements IfxStream {

    protected final ChangeEventSourceContext context;
    protected final DbzTransactionEngine engine;
    protected final List<IfmxStreamListener> listeners = new CopyOnWriteArrayList<>();
    protected final List<IfxStreamException> exceptions = new ArrayList<>();
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
            exceptions.add(new IfxStreamException("SQL exception caught during engine initialization ", e));
            return;
        }
        catch (IfxStreamException e) {
            exceptions.add(e);
            return;
        }

        while (context.isRunning() && (!stopOnError || exceptions.isEmpty())) {
            try {
                for (IfmxStreamRecord sr : engine.getRecords()) {
                    IfmxStreamRecord pr = engine.processRecord(sr);
                    if (pr != null) {
                        for (IfmxStreamListener listener : listeners) {
                            try {
                                listener.accept(pr);
                            }
                            catch (SQLException e) {
                                exceptions.add(new IfxStreamException("SQL exception occurred in listener [%s] while processing record [%s]".formatted(listener, pr), e));
                            }
                            catch (IfxStreamException e) {
                                exceptions.add(e);
                            }
                        }
                    }
                }
            }
            catch (SQLException e) {
                exceptions.add(new IfxStreamException("SQL exception caught processing records ", e));
            }
            catch (IfxStreamException e) {
                exceptions.add(e);
            }
        }
    }

    @Override
    public IfxStream addListener(IfmxStreamListener streamListener) {
        listeners.add(streamListener);
        return this;
    }

    @Override
    public List<IfxStreamException> getExceptions() {
        return exceptions;
    }

    @Override
    public void close() throws Exception {
        engine.close();
    }
}
