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
package com.couchbase.client.spring.cache.wiring;

import static org.junit.Assert.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.spring.cache.CacheBuilder;
import com.couchbase.client.spring.cache.CouchbaseCacheManager;
import com.couchbase.client.spring.cache.wiring.CachedService;
import com.couchbase.client.spring.cache.wiring.javaConfig.CacheEnabledTestConfiguration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Common test case for the wiring and execution of a {@link Cacheable}-annotated {@link CachedService}.
 *
 * @author Simon Baslé
 */
public class AbstractCouchbaseCacheWiringTest {

  @Autowired
  public Cluster cluster;

  @Autowired
  public Bucket bucket;

  @Autowired
  public CacheBuilder defaultBuilder;

  @Autowired
  public CouchbaseCacheManager cacheManager;

  @Autowired
  public CachedService service;

  @After
  public void cleanCache() {
    Cache cache = cacheManager.getCache(CacheEnabledTestConfiguration.DATA_CACHE_NAME);
    cache.clear();
  }



  @Test
  public void testCachingOccurs() {
    service.resetCounters();
    assertEquals(0L, service.getCounterGetData());

    CachedService.Data data = service.getData("toto", "abc");
    assertNotNull(data);
    assertEquals(1L, service.getCounterGetData());

    CachedService.Data cachedData = service.getData("toto", "abc");
    assertNotNull(cachedData);
    assertNotSame(data, cachedData);
    assertEquals(data, cachedData);
    assertEquals(1L, service.getCounterGetData());

    SimpleKey cacheKey = new SimpleKey("toto", "abc");
    String expectedCouchbaseKey = cbKey(cacheKey);
    assertTrue(bucket.exists(expectedCouchbaseKey));
  }

  @Test
  public void testCachingWithSpecificKeyOccursIfUnlessNotMet() {
    String criteriaA = "tata";
    String criteriaB = "abcdef";
    int size = 8;

    service.resetCounters();
    CachedService.Data data = service.getDataWithSize(criteriaA, criteriaB, size);
    CachedService.Data cachedData = service.getDataWithSize(criteriaA, criteriaB, size);

    assertNotSame(data, cachedData);
    assertEquals(data, cachedData);
    assertEquals(1, service.getCounterGetDataWithSize());

    SimpleKey unexpectedCacheKey = new SimpleKey(criteriaA, criteriaB, size);
    String unexpectedCouchbaseKey = cbKey(unexpectedCacheKey);
    String expectedCouchbaseKey = cbKey(criteriaA);
    assertFalse("data was wrongfully cached with a generic key " + unexpectedCouchbaseKey, bucket.exists(unexpectedCouchbaseKey));
    assertTrue("data was not cached with the specific key " + expectedCouchbaseKey, bucket.exists(expectedCouchbaseKey));
  }

  @Test
  public void testCachingWithSpecificKeyDoesntOccurIfUnlessMet() {
    String criteriaA = "tataTooShort";
    String criteriaB = "abcdef";
    int size = 3;

    service.resetCounters();
    CachedService.Data data = service.getDataWithSize(criteriaA, criteriaB, size);
    CachedService.Data cachedData = service.getDataWithSize(criteriaA, criteriaB, size);

    assertNotSame(data, cachedData);
    assertEquals(data, cachedData);
    assertEquals(2, service.getCounterGetDataWithSize());

    SimpleKey unexpectedCacheKey = new SimpleKey(criteriaA, criteriaB, size);
    String unexpectedCouchbaseKey = cbKey(unexpectedCacheKey);
    String expectedCouchbaseKey = cbKey(criteriaA);
    assertFalse("data was wrongfully cached with a generic key " + unexpectedCouchbaseKey, bucket.exists(unexpectedCouchbaseKey));
    assertFalse("data was wrongfully cached with the specific key " + expectedCouchbaseKey, bucket.exists(expectedCouchbaseKey));
  }

  @Test
  public void testCachingWithKeyThenEvict() {
    String cacheKey = "cacheThenEvict";
    String criteriaB = "irrelevant";
    int size = 100;

    service.resetCounters();
    CachedService.Data data = service.getDataWithSize(cacheKey, criteriaB, size);

    assertTrue(bucket.exists(cbKey(cacheKey)));

    service.getDataWithSize(cacheKey, criteriaB, size);

    assertEquals(1, service.getCounterGetDataWithSize());

    service.store(data);

    assertFalse("Data wasn't evicted from cache", bucket.exists(cbKey(cacheKey)));
  }

  @Test
  public void testCacheableMethodWithUnknownCacheNameFails() {
    try {
      service.getDataWrongCache("criteria");
      fail("Cacheable method with unknown cache name expected to fail");
    } catch (IllegalArgumentException e) {
      if (e.getMessage() == null || !e.getMessage().contains("Cannot find cache named 'wrongCache'")) {
        fail("Expected IllegalArgumentException to state it cannot find cache named 'wrongCache'");
      }
    }
  }


  private static String cbKey(Object cacheKey) {
    return "cache:" + CacheEnabledTestConfiguration.DATA_CACHE_NAME
        + ":" + String.valueOf(cacheKey);
  }
}
