/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix.stream;

import java.io.Serializable;
import java.util.stream.StreamSupport;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

import com.informix.jdbc.stream.api.StreamRecord;

import io.debezium.DebeziumException;

public class DbzCacheEntryExpiredListener implements Serializable, CacheEntryExpiredListener<Long, StreamRecord> {
    public DbzCacheEntryExpiredListener() {
    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends Long, ? extends StreamRecord>> cacheEntryEvents) throws CacheEntryListenerException {
        throw new DebeziumException("Transaction cache entries expired: %s".formatted(
                StreamSupport.stream(cacheEntryEvents.spliterator(), true).map(CacheEntryEvent::getKey).map(Long::valueOf).sorted()));
    }
}
