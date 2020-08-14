/*
 * Copyright (C) 2015 Couchbase Inc., the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.spring.cache.reactive;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.spring.cache.TestConfiguration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the ReactiveCouchbaseCache class and verifies its functionality.
 *
 * @author Michael Nitschinger
 * @author Konrad Król
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class ReactiveCouchbaseCacheTests {
    
    /**
     * Contains a reference to the actual CouchbaseClient.
     */
    @Autowired
    private Bucket client;
    
    /**
     * Simple name of the cache bucket to create.
     */
    private String cacheName = "test";
    
    /**
     * Tests the basic Cache construction functionality.
     */
    @Test
    public void testConstruction() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        
        assertEquals(cacheName, cache.getName());
        assertEquals(client, cache.getNativeCache());
    }
    
    /**
     * Verifies set() and get() of cache objects.
     */
    @Test
    public void testGetSet() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        
        String key = "couchbase-cache-test";
        String value = "Hello World!";
        cache.put(key, Mono.just(value));
        
        String stored = (String) (((Mono) cache.get(key).get())).block();
        assertNotNull(stored);
        assertEquals(value, stored);
    }
    
    /**
     * Verifies set() with TTL value.
     */
    @Test
    public void testSetWithTtl() throws InterruptedException {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client, 1); // cache for 1 second
        
        String key = "couchbase-cache-test";
        String value = "Hello World!";
        cache.put(key, Mono.just(value));
        
        // wait for TTL to expire (double time of TTL)
        Thread.sleep(2000);
        
        ValueWrapper stored = cache.get(key);
        assertNull(stored);
    }
    
    @Test
    public void testGetSetWithCast() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        
        String key = "couchbase-cache-user";
        User user = new User();
        user.firstname = "Michael";
        
        cache.put(key, Mono.just(user));
        
        User loaded = (User) (((Mono) cache.get(key).get())).block();
        assertNotNull(loaded);
        assertEquals(user.firstname, loaded.firstname);
    }
    
    /**
     * Verifies the deletion of cache objects.
     *
     * @throws Exception
     */
    @Test
    public void testEvict() throws Exception {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        
        String key = "couchbase-cache-test";
        String value = "Hello World!";
        
        cache.put(key, Mono.just(value));
        Thread.sleep(10);
        
        cache.evict(key);
        
        Thread.sleep(10);
        Object result = cache.get(key);
        assertNull(result);
    }
    
    /**
     * Putting into cache on the same key not null value, and then null value,
     * results in null object
     */
    @Test
    public void testSettingNullAndGetting() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        
        String key = "couchbase-cache-test";
        String value = "Hello World!";
        
        cache.put(key, Mono.just(value));
        cache.put(key, null);
        
        assertNull(cache.get(key));
    }
    
    /**
     * Putting into cache on the same key not null value and then clearing the cache,
     * results in null object
     */
    @Test
    public void testClearingUsingViewDoesntImpactUnrelatedDocuments() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        assertFalse("Expected flush to be disabled by default", cache.getCouchbaseCache().getAlwaysFlush());
        
        String key = "couchbase-cache-test";
        String unrelatedId = "randomKey";
        String value = "Hello World!";
        
        cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
        cache.put(key, Mono.just(value));
        cache.clear();
        
        assertNotNull(cache.getNativeCache().remove(unrelatedId));
        assertNull(cache.get(key));
    }
    
    @Test
    public void testClearingEmptyNameCacheUsingViewDoesntImpactUnrelatedDocuments() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(null, client);
        assertFalse("Expected flush to be disabled by default", cache.getCouchbaseCache().getAlwaysFlush());
        
        String key = "couchbase-cache-test";
        String unrelatedId = "randomKey:in:parts";
        String value = "Hello World!";
        
        cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
        cache.put(key, Mono.just(value));
        cache.clear();
        
        assertNotNull(cache.getNativeCache().remove(unrelatedId));
        assertNull(cache.get(key));
    }
    
    /**
     * Putting into cache on the same key not null value and then clearing the cache,
     * results in null object.
     * <p>
     * As this is quite a dangerous method of clearing a cache (with side effects, namely it empties the whole
     * bucket, including unrelated data), this test has been disabled by default. If you're sure it is safe to
     * execute, comment the @Ignore annotation below.
     */
    @Ignore("Flush clearing test disabled, see test comment.")
    @Test
    public void testClearingUsingFlushImpactsUnrelatedDocuments() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        cache.getCouchbaseCache().setAlwaysFlush(true);
        
        String key = "couchbase-cache-test";
        String unrelatedId = "randomKey";
        String value = "Hello World!";
        
        cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
        cache.put(key, Mono.just(value));
        cache.clear();
        
        try {
            cache.getNativeCache().remove(unrelatedId);
            fail(unrelatedId + " is expected to have been flushed");
        } catch (DocumentDoesNotExistException e) {
            //success
        }
        assertNull(cache.get(key));
    }
    
    @Test
    public void testClearingEmptyCacheUsingViewSucceeds() {
        ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache("emptyCache", client);
        String unrelatedId = "unrelated";
        cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
        
        try {
            cache.clear();
        } catch (NoSuchElementException e) {
            fail("Cache clearing failed on empty cache: " + e.toString());
        }
        assertTrue(cache.getNativeCache().exists(unrelatedId));
    }
    
    @Test
    public void testCallingSyncGetInParallel() throws ExecutionException, InterruptedException {
        final String key = "getWithValueLoader";
        final ReactiveCouchbaseCache cache = new ReactiveCouchbaseCache(cacheName, client);
        try {
            cache.evict(key);
        } catch (DocumentDoesNotExistException e) {
        }
        final AtomicInteger count = new AtomicInteger(0);
        final Callable<Mono<String>> valueLoader = new Callable<Mono<String>>() {
            @Override
            public Mono<String> call() throws Exception {
                return Mono.just("value" + count.getAndIncrement());
            }
        };
        Runnable task = new Runnable() {
            @Override
            public void run() {
                cache.get(key, valueLoader);
            }
        };
        
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> future1 = executorService.submit(task);
        Future<?> future2 = executorService.submit(task);
        future1.get();
        future2.get();
        
        assertEquals(1, count.get());
        ValueWrapper w = cache.get(key);
        assertNotNull(w);
        assertEquals("value0", ((Mono) w.get()).block());
    }
    
    static class User implements Serializable {
        public String firstname;
    }
    
}
