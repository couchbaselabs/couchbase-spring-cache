package com.couchbase.client.spring.cache.reactive;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.spring.cache.CouchbaseCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import reactor.core.publisher.Mono;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class ReactiveCouchbaseCache implements Cache {
    
    private static final Logger logger = LoggerFactory.getLogger(ReactiveCouchbaseCache.class);
    
    private CouchbaseCache couchbaseCache;
    
    /**
     * Construct the cache and pass in the {@link Bucket} instance.
     *
     * @param name   the name of the cache reference.
     * @param client the Bucket instance.
     */
    public ReactiveCouchbaseCache(final String name, final Bucket client) {
        couchbaseCache = new CouchbaseCache(name, client);
    }
    
    /**
     * Construct the cache and pass in the {@link Bucket} instance.
     *
     * @param name   the name of the cache reference.
     * @param client the Bucket instance.
     * @param ttl    TTL value for objects in this cache
     */
    public ReactiveCouchbaseCache(final String name, final Bucket client, int ttl) {
        couchbaseCache = new CouchbaseCache(name, client, ttl);
    }
    
    /**
     * Returns the name of the cache.
     *
     * @return the name of the cache.
     */
    @Override
    public final String getName() {
        return couchbaseCache.getName();
    }
    
    /**
     * Returns the underlying SDK {@link Bucket} instance.
     *
     * @return the underlying Bucket instance.
     */
    @Override
    public final Bucket getNativeCache() {
        return couchbaseCache.getNativeCache();
    }
    
    @Override
    public final ValueWrapper get(final Object key) {
        ValueWrapper valueWrapper = couchbaseCache.get(key);
        
        if (valueWrapper == null) {
            return null;
        }
        
        Object object = valueWrapper.get();
        
        return new SimpleValueWrapper(Mono.just(object));
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The <code>clazz</code> is expected to be implementing {@link java.io.Serializable}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final <T> T get(final Object key, final Class<T> clazz) {
        throw new IllegalArgumentException("Not implemented yet.");
    }
    
    /**
     * Return the value to which this cache maps the specified key, obtaining
     * that value from {@code valueLoader} if necessary. This method provides
     * a simple substitute for the conventional "if cached, return; otherwise
     * create, cache and return" pattern.
     * <p>If possible, implementations should ensure that the loading operation
     * is synchronized so that the specified {@code valueLoader} is only called
     * once in case of concurrent access on the same key.
     * <p>If the {@code valueLoader} throws an exception, it is wrapped in
     * a {@link RuntimeException}
     *
     * @param key         the key whose associated value is to be returned
     * @param valueLoader callable that is going to be executed only if need to.
     * @return the value to which this cache maps the specified key
     * @throws RuntimeException if the {@code valueLoader} throws an exception
     * @since 4.3
     */
    @Override
    public <T> T get(final Object key, final Callable<T> valueLoader) {
        ValueWrapper valueWrapper = get(key);
        if (valueWrapper == null && valueLoader != null) {
            synchronized (couchbaseCache) {
                valueWrapper = get(key);
                if (valueWrapper == null) {
                    try {
                        T value = valueLoader.call();
                        put(key, value);
                        return value;
                    } catch (ValueRetrievalException ex) {
                        throw ex;
                    } catch (Exception e) {
                        throw new ValueRetrievalException(key, valueLoader, e);
                    }
                }
            }
        }
        
        if (valueWrapper != null) {
            return (T) valueWrapper.get();
        }
        return null;
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Values of Mono are expected to be {@link java.io.Serializable}.
     *
     * @param key   the Key of the storable object.
     * @param value the Serializable object to store.
     */
    @Override
    public final void put(final Object key, final Object value) {
        
        if (value instanceof Mono) {
            Mono<Object> mono = (Mono) value;
            mono
                    .doOnSuccess(new Consumer<Object>() {
                        public void accept(Object v) {
                            couchbaseCache.put(key, v);
                        }
                    })
                    .doOnError(new Consumer<Throwable>() {
                        public void accept(Throwable throwable) {
                            // incomming mono is errored out, do nothing with cache.
                            // we need this handler to prevent 'error no handled' stack trace in the logs.
                        }
                    })
                    .subscribe();
        } else {
            couchbaseCache.put(key, value);
        }
    }
    
    @Override
    public final void evict(final Object key) {
        couchbaseCache.evict(key);
    }
    
    /**
     * Clear the complete cache.
     */
    @Override
    public final void clear() {
        couchbaseCache.clear();
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note that the atomicity in this Couchbase implementation is guaranteed for the insertion part. The detection of
     * the pre-existing key and thus skipping of the insertion is atomic. However, in such a case a get will have to be
     * performed to retrieve the pre-existing value. This get is not atomic and could see the key as deleted, in which
     * case it will return a {@link ValueWrapper} with a null content.
     */
    @Override
    public ValueWrapper putIfAbsent(final Object key, Object value) {
        if (value instanceof Mono) {
            
            Mono<Object> mono = (Mono) value;
            mono
                    .doOnSuccess(new Consumer<Object>() {
                        public void accept(Object v) {
                            ValueWrapper valueWrapper = couchbaseCache.putIfAbsent(key, v);
                        }
                    })
                    .doOnError(new Consumer<Throwable>() {
                        public void accept(Throwable throwable) {
                            // incomming mono is errored out, do nothing with cache.
                            // we need this handler to prevent 'error no handled' stack trace in the logs.
                        }
                    })
                    .subscribe();
            
            return new SimpleValueWrapper(Mono.just(value));
            
        } else {
            return couchbaseCache.putIfAbsent(key, value);
        }
    }
    
    public CouchbaseCache getCouchbaseCache() {
        return couchbaseCache;
    }
}
