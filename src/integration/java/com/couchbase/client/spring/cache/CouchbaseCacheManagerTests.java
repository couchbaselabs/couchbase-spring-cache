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

package com.couchbase.client.spring.cache;

import java.util.*;

import com.couchbase.client.java.Bucket;
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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

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
public class CouchbaseCacheManagerTests {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  /**
   * Contains a reference to the actual underlying {@link Bucket}.
   */
  @Autowired
  private Bucket client;

  private CacheBuilder defaultCacheBuilder;

  @Before
  public void setup() {
    this.defaultCacheBuilder = CacheBuilder.newInstance(client);
  }

  /**
   * Test statically declaring and loading a cache with default expiry.
   */
  @Test
  public void testCacheInit() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(defaultCacheBuilder, "test");
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
    CouchbaseCacheManager manager = new CouchbaseCacheManager(defaultCacheBuilder, "test");
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
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(100), "cache1", "cache2");
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
    Map<String, CacheBuilder> cacheConfigs = new LinkedHashMap<String, CacheBuilder>();
    cacheConfigs.put("cache1", CacheBuilder.newInstance(client).withExpirationInMillis(100));
    cacheConfigs.put("cache2", CacheBuilder.newInstance(client).withExpirationInMillis(200));
    CouchbaseCacheManager manager = new CouchbaseCacheManager(cacheConfigs);
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("cache1", "cache2"));
    assertThat(manager.getCacheNames().size(), equalTo(2));

    assertCache(manager, "cache1", client, 100);
    assertCache(manager, "cache2", client, 200);
  }

  @Test
  public void testCacheInitWithSingleConfig() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            Collections.singletonMap("test",
                    CacheBuilder.newInstance(client).withExpirationInMillis(100)));

    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));

    assertCache(manager, "test", client, 100);
  }

  @Test
  public void testCacheInitWithSingleConfigAndNoTtl() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            Collections.singletonMap("test",
                    CacheBuilder.newInstance(client)));
    //null ttl in config should result in using the default ttl's value at afterPropertiesSet
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "test", client, 0);
  }

  @Test
  public void testCacheInitWithThreeConfigs() {
    Map<String, CacheBuilder> cacheConfigs = new LinkedHashMap<String, CacheBuilder>();
    cacheConfigs.put("cache1", CacheBuilder.newInstance(client).withExpirationInMillis(100));
    cacheConfigs.put("cache2", CacheBuilder.newInstance(client).withExpirationInMillis(200));
    cacheConfigs.put("cache3", CacheBuilder.newInstance(client).withExpirationInMillis(300));
    CouchbaseCacheManager manager = new CouchbaseCacheManager(cacheConfigs);

    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("cache1", "cache2", "cache3"));
    assertThat(manager.getCacheNames().size(), equalTo(3));
    assertCache(manager, "cache1", client, 100);
    assertCache(manager, "cache2", client, 200);
    assertCache(manager, "cache3", client, 300);
  }

  @Test
  public void testCacheInitWithConfigIgnoresDuplicates() {
    Map<String, CacheBuilder> cacheConfigs = new LinkedHashMap<String, CacheBuilder>();
    cacheConfigs.put("test", CacheBuilder.newInstance(client).withExpirationInMillis(100));
    cacheConfigs.put("test", CacheBuilder.newInstance(client).withExpirationInMillis(200));
    cacheConfigs.put("test", CacheBuilder.newInstance(client).withExpirationInMillis(300));

    CouchbaseCacheManager manager = new CouchbaseCacheManager(cacheConfigs);
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "test", client, 300);
  }

  @Test
  public void testCacheInitWithConfigIgnoresNullVararg() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(400));
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
    CouchbaseCacheManager manager = new CouchbaseCacheManager(defaultCacheBuilder);
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames().size(), equalTo(0));
    manager.getCache("test");

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "test", client, 0); // default TTL value
  }

  @Test
  public void testDynamicCacheInitWithTtl() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        defaultCacheBuilder.withExpirationInMillis(20));
    manager.afterPropertiesSet();
    Cache expiringCache = manager.getCache("testExpiring");

    assertNotNull(expiringCache);
    assertThat(manager.getCacheNames(), hasItems("testExpiring"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "testExpiring", client, 20);
  }

  @Test
  public void disableDynamicMode() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(20));
    manager.afterPropertiesSet();

    manager.setDefaultCacheBuilder(null);

    assertThat(manager.getCache("dynamicCache"), nullValue());
    assertThat(manager.getCacheNames().size(), equalTo(0));
  }

  @Test
  public void prepareCache() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(20));
    manager.prepareCache("test");
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "test", client, 20);
  }

  @Test
  public void prepareCacheNoBuilder() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(20), "test");

    thrown.expect(IllegalStateException.class);
    manager.prepareCache("another");
  }

  @Test
  public void prepareCacheCustomBuilder() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(20));
    manager.prepareCache("test", CacheBuilder.newInstance(client).withExpirationInMillis(400));
    manager.afterPropertiesSet();

    assertThat(manager.getCacheNames(), hasItems("test"));
    assertThat(manager.getCacheNames().size(), equalTo(1));
    assertCache(manager, "test", client, 400);
  }

  @Test
  public void prepareCacheCacheManagerInitialized() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
            defaultCacheBuilder.withExpirationInMillis(20));
    manager.afterPropertiesSet();

    thrown.expect(IllegalStateException.class);
    manager.prepareCache("test", CacheBuilder.newInstance(client).withExpirationInMillis(400));
  }

  private static void assertCache(CacheManager cacheManager, String name, Bucket client, int ttl) {
    Cache actual = cacheManager.getCache(name);
    assertThat(actual, not(nullValue()));
    assertThat(actual.getName(), equalTo(name));
    assertThat(actual, instanceOf(CouchbaseCache.class));
    CouchbaseCache couchbaseCache = (CouchbaseCache) actual;
    assertThat(couchbaseCache.getNativeCache(), equalTo(client));
    assertThat(couchbaseCache.getTtl(), equalTo(ttl));
  }

}
