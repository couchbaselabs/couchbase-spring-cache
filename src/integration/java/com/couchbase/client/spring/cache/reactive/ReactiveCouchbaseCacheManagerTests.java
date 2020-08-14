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
import com.couchbase.client.spring.cache.TestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Verifies the correct functionality of the CouchbaseCacheManager,
 * loading a static set of caches at initialization or dynamically creating caches.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Stéphane Nicoll
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class ReactiveCouchbaseCacheManagerTests {
    
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    
    /**
     * Contains a reference to the actual underlying {@link Bucket}.
     */
    @Autowired
    private Bucket client;
    
    private ReactiveCacheBuilder defaultCacheBuilder;
    
    @Before
    public void setup() {
        this.defaultCacheBuilder = ReactiveCacheBuilder.newInstance(client);
    }
    
    /**
     * Test statically declaring and loading a cache with default expiry.
     */
    @Test
    public void testCacheInit() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(defaultCacheBuilder, "test");
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 0); // default TTL value
    }
    
    /**
     * Test statically declaring and loading a cache won't allow for dynamic creation later
     */
    @Test
    public void testStaticCacheInitOnlyCreatesKnownCaches() {
        
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(defaultCacheBuilder,
                "test");
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        
        Cache cache = manager.getCache("test");
        assertNotNull(cache);
        
        Cache invalidCache = manager.getCache("invalid");
        assertNull(invalidCache);
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
    }
    
    /**
     * Test statically declaring and loading 2 caches with a common non-zero TTL value.
     */
    @Test
    public void testCacheInitWithCommonTtl() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(100), "cache1", "cache2");
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("cache1", "cache2"));
        assertThat(manager.getCacheNames().size(), equalTo(2));
        
        assertCache(manager, "cache1", client, 100);
        assertCache(manager, "cache2", client, 100);
    }
    
    /**
     * Test statically declaring and loading 2 caches with custom TTL values.
     */
    @Test
    public void testCacheInitWithCustomTtl() {
        Map<String, ReactiveCacheBuilder> cacheConfigs = new LinkedHashMap<String, ReactiveCacheBuilder>();
        cacheConfigs.put("cache1", ReactiveCacheBuilder.newInstance(client).withExpiration(100));
        cacheConfigs.put("cache2", ReactiveCacheBuilder.newInstance(client).withExpiration(200));
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(cacheConfigs);
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("cache1", "cache2"));
        assertThat(manager.getCacheNames().size(), equalTo(2));
        
        assertCache(manager, "cache1", client, 100);
        assertCache(manager, "cache2", client, 200);
    }
    
    @Test
    public void testCacheInitWithSingleConfig() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                Collections.singletonMap("test",
                        ReactiveCacheBuilder.newInstance(client).withExpiration(100)));
        
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        
        assertCache(manager, "test", client, 100);
    }
    
    @Test
    public void testCacheInitWithSingleConfigAndNoTtl() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                Collections.singletonMap("test",
                        ReactiveCacheBuilder.newInstance(client)));
        //null ttl in config should result in using the default ttl's value at afterPropertiesSet
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 0);
    }
    
    @Test
    public void testCacheInitWithThreeConfigs() {
        Map<String, ReactiveCacheBuilder> cacheConfigs = new LinkedHashMap<String, ReactiveCacheBuilder>();
        cacheConfigs.put("cache1", ReactiveCacheBuilder.newInstance(client).withExpiration(100));
        cacheConfigs.put("cache2", ReactiveCacheBuilder.newInstance(client).withExpiration(200));
        cacheConfigs.put("cache3", ReactiveCacheBuilder.newInstance(client).withExpiration(300));
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(cacheConfigs);
        
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("cache1", "cache2", "cache3"));
        assertThat(manager.getCacheNames().size(), equalTo(3));
        assertCache(manager, "cache1", client, 100);
        assertCache(manager, "cache2", client, 200);
        assertCache(manager, "cache3", client, 300);
    }
    
    @Test
    public void testCacheInitWithConfigIgnoresDuplicates() {
        Map<String, ReactiveCacheBuilder> cacheConfigs = new LinkedHashMap<String, ReactiveCacheBuilder>();
        cacheConfigs.put("test", ReactiveCacheBuilder.newInstance(client).withExpiration(100));
        cacheConfigs.put("test", ReactiveCacheBuilder.newInstance(client).withExpiration(200));
        cacheConfigs.put("test", ReactiveCacheBuilder.newInstance(client).withExpiration(300));
        
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(cacheConfigs);
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 300);
    }
    
    @Test
    public void testCacheInitWithConfigIgnoresNullVararg() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(400));
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames().size(), equalTo(0));
        manager.getCache("test");
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 400);
    }
    
    /**
     * Test dynamic cache creation, with changing default ttl.
     */
    @Test
    public void testDynamicCacheInitWithoutTtl() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(defaultCacheBuilder);
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames().size(), equalTo(0));
        manager.getCache("test");
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 0); // default TTL value
    }
    
    @Test
    public void testDynamicCacheInitWithTtl() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20));
        manager.afterPropertiesSet();
        Cache expiringCache = manager.getCache("testExpiring");
        
        assertNotNull(expiringCache);
        assertThat(manager.getCacheNames(), hasItems("testExpiring"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "testExpiring", client, 20);
    }
    
    @Test
    public void disableDynamicMode() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20));
        manager.afterPropertiesSet();
        
        manager.setDefaultCacheBuilder(null);
        
        assertThat(manager.getCache("dynamicCache"), nullValue());
        assertThat(manager.getCacheNames().size(), equalTo(0));
    }
    
    @Test
    public void prepareCache() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20));
        manager.prepareCache("test");
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 20);
    }
    
    @Test
    public void prepareCacheNoBuilder() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20), "test");
        
        thrown.expect(IllegalStateException.class);
        manager.prepareCache("another");
    }
    
    @Test
    public void prepareCacheCustomBuilder() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20));
        manager.prepareCache("test", ReactiveCacheBuilder.newInstance(client).withExpiration(400));
        manager.afterPropertiesSet();
        
        assertThat(manager.getCacheNames(), hasItems("test"));
        assertThat(manager.getCacheNames().size(), equalTo(1));
        assertCache(manager, "test", client, 400);
    }
    
    @Test
    public void prepareCacheCacheManagerInitialized() {
        ReactiveCouchbaseCacheManager manager = new ReactiveCouchbaseCacheManager(
                defaultCacheBuilder.withExpiration(20));
        manager.afterPropertiesSet();
        
        thrown.expect(IllegalStateException.class);
        manager.prepareCache("test", ReactiveCacheBuilder.newInstance(client).withExpiration(400));
    }
    
    private static void assertCache(CacheManager cacheManager, String name, Bucket client, int ttl) {
        Cache actual = cacheManager.getCache(name);
        assertThat(actual, not(nullValue()));
        assertThat(actual.getName(), equalTo(name));
        assertThat(actual, instanceOf(ReactiveCouchbaseCache.class));
        ReactiveCouchbaseCache couchbaseCache = (ReactiveCouchbaseCache) actual;
        assertThat(couchbaseCache.getNativeCache(), equalTo(client));
        assertThat(couchbaseCache.getCouchbaseCache().getTtl(), equalTo(ttl));
    }
    
}
