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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.CollectionUtils;

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

  /**
   * Contains a reference to the actual underlying {@link Bucket}.
   */
  @Autowired
  private Bucket client;

  /**
   * Test statically declaring and loading a cache with default expiry.
   */
  @Test
  public void testStaticCacheInit() {
    Set<String> cacheNames = Collections.singleton("test");
    CouchbaseCacheManager manager = new CouchbaseCacheManager(client, cacheNames);
    manager.afterPropertiesSet();
    
    assertEquals(cacheNames, manager.getCacheNames());
    
    Cache cache = manager.getCache("test");
    
    assertNotNull(cache);
    
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(cache.getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 0); // default TTL value
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }

  /**
   * Test statically declaring and loading a cache won't allow for dynamic creation later
   */
  @Test
  public void testStaticCacheOnlyCreatesKnownCaches() {
    Set<String> cacheNames = Collections.singleton("test");
    CouchbaseCacheManager manager = new CouchbaseCacheManager(client, cacheNames);
    manager.afterPropertiesSet();

    assertEquals(cacheNames, manager.getCacheNames());

    Cache cache = manager.getCache("test");
    assertNotNull(cache);

    Cache invalidCache = manager.getCache("invalid");
    assertNull(invalidCache);
    assertEquals(cacheNames, manager.getCacheNames());
  }

  /**
   * Test statically declaring and loading 2 caches with a common non-zero TTL value.
   */
  @Test
  public void testStaticCacheInitWithCommonTtl() {
    Set<String> names = new HashSet<String>();
    names.add("cache1");
    names.add("cache2");

    CouchbaseCacheManager manager = new CouchbaseCacheManager(client, 100, names);
    manager.setDefaultTtl(300); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    
    assertEquals(names, manager.getCacheNames());
    
    Cache cache1 = manager.getCache("cache1");
    Cache cache2 = manager.getCache("cache2");
    
    assertNotNull(cache1);
    assertNotNull(cache2);
    
    assertEquals(cache1.getClass(), CouchbaseCache.class);
    assertEquals(cache2.getClass(), CouchbaseCache.class);
    
    assertEquals(cache1.getName(), "cache1");
    assertEquals(cache2.getName(), "cache2");
    
    assertEquals(((CouchbaseCache) cache1).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache2).getTtl(), 100);
    
    assertEquals(((CouchbaseCache) cache1).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache2).getNativeCache(), client);
  }

  /**
   * Test statically declaring and loading 2 caches with custom TTL values.
   */
  @Test
  public void testStaticCacheInitWithCustomTtl() {
    HashMap<String, Integer> withTtl = new HashMap<String, Integer>();
    withTtl.put("cache1", 100);
    withTtl.put("cache2", 200);

    CouchbaseCacheManager manager = new CouchbaseCacheManager(client, withTtl);
    manager.setDefaultTtl(300); //to make sure defaultTtl has been overridden
    manager.afterPropertiesSet();

    assertEquals(withTtl.keySet(), manager.getCacheNames());

    Cache cache1 = manager.getCache("cache1");
    Cache cache2 = manager.getCache("cache2");

    assertNotNull(cache1);
    assertNotNull(cache2);

    assertEquals(cache1.getClass(), CouchbaseCache.class);
    assertEquals(cache2.getClass(), CouchbaseCache.class);

    assertEquals(cache1.getName(), "cache1");
    assertEquals(cache2.getName(), "cache2");

    assertEquals(((CouchbaseCache) cache1).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache2).getTtl(), 200);

    assertEquals(((CouchbaseCache) cache1).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache2).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithSingleConfig() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("test", client, 100));
    manager.setDefaultTtl(400); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    Cache cache = manager.getCache("test");

    assertEquals(Collections.singleton("test"), manager.getCacheNames());
    assertNotNull(cache);
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(cache.getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithSingleConfigAndNoTtl() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("test", client, null));
    //null ttl in config should result in using the default ttl's value at afterPropertiesSet
    manager.setDefaultTtl(400);
    manager.afterPropertiesSet();
    Cache cache = manager.getCache("test");

    assertEquals(Collections.singleton("test"), manager.getCacheNames());
    assertNotNull(cache);
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(cache.getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 400);
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithTwoConfigs() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("cache1", client, 100),
        new CouchbaseCacheManager.Config("cache2", client, 200)
    );
    manager.setDefaultTtl(400); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    Cache cache1 = manager.getCache("cache1");
    Cache cache2 = manager.getCache("cache2");

    assertEquals(new HashSet<String>(Arrays.asList("cache1", "cache2")), manager.getCacheNames());
    assertNotNull(cache1);
    assertNotNull(cache2);
    assertEquals(cache1.getClass(), CouchbaseCache.class);
    assertEquals(cache2.getClass(), CouchbaseCache.class);
    assertEquals(cache1.getName(), "cache1");
    assertEquals(cache2.getName(), "cache2");
    assertEquals(((CouchbaseCache) cache1).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache2).getTtl(), 200);
    assertEquals(((CouchbaseCache) cache1).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache2).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithThreeConfigs() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("cache1", client, 100),
        new CouchbaseCacheManager.Config("cache2", client, 200),
        new CouchbaseCacheManager.Config("cache3", client, 300)
    );
    manager.setDefaultTtl(400); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    Cache cache1 = manager.getCache("cache1");
    Cache cache2 = manager.getCache("cache2");
    Cache cache3 = manager.getCache("cache3");

    assertEquals(new HashSet<String>(Arrays.asList("cache1", "cache2", "cache3")), manager.getCacheNames());
    assertNotNull(cache1);
    assertNotNull(cache2);
    assertNotNull(cache3);
    assertEquals(cache1.getClass(), CouchbaseCache.class);
    assertEquals(cache2.getClass(), CouchbaseCache.class);
    assertEquals(cache3.getClass(), CouchbaseCache.class);
    assertEquals(cache1.getName(), "cache1");
    assertEquals(cache2.getName(), "cache2");
    assertEquals(cache3.getName(), "cache3");
    assertEquals(((CouchbaseCache) cache1).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache2).getTtl(), 200);
    assertEquals(((CouchbaseCache) cache3).getTtl(), 300);
    assertEquals(((CouchbaseCache) cache1).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache2).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache3).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithConfigIgnoresDuplicates() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("test", client, 100),
        new CouchbaseCacheManager.Config("test", client, 200),
        new CouchbaseCacheManager.Config("test", client, 300)
    );
    manager.setDefaultTtl(400); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    Cache cache = manager.getCache("test");

    assertEquals(Collections.singleton("test"), manager.getCacheNames());
    assertNotNull(cache);
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(cache.getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 300); //latest config wins
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }

  @Test
  public void testStaticCacheInitWithConfigIgnoresNullVararg() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(
        new CouchbaseCacheManager.Config("test", client, null),
        null
    );
    manager.setDefaultTtl(400); //to make sure defaultTtl being later overridden isn't taken into account
    manager.afterPropertiesSet();
    Cache cache = manager.getCache("test");

    assertEquals(Collections.singleton("test"), manager.getCacheNames());
    assertNotNull(cache);
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(cache.getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 400);
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }

  /**
   * Test dynamic cache creation, with changing default ttl.
   */
  @Test
  public void testDynamicCacheInit() {
    CouchbaseCacheManager manager = new CouchbaseCacheManager(client);
    manager.afterPropertiesSet();

    assertEquals(Collections.emptySet(), manager.getCacheNames());

    Cache cache = manager.getCache("test");

    assertNotNull(cache);
    assertEquals(Collections.singleton("test"), manager.getCacheNames());
    assertEquals(CouchbaseCache.class, cache.getClass());
    assertEquals("test", cache.getName());
    assertEquals(0, ((CouchbaseCache) cache).getTtl()); // default TTL value
    assertEquals(client, ((CouchbaseCache) cache).getNativeCache());

    manager.setDefaultTtl(20);
    Cache expiringCache = manager.getCache("testExpiring");

    assertNotNull(expiringCache);
    assertEquals(new HashSet<String>(Arrays.asList("test", "testExpiring")), manager.getCacheNames());
    assertEquals(CouchbaseCache.class, expiringCache.getClass());
    assertEquals("testExpiring", expiringCache.getName());
    assertEquals(20, ((CouchbaseCache) expiringCache).getTtl()); // updated default TTL value
    assertEquals(client , ((CouchbaseCache) expiringCache).getNativeCache());
  }

}
