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


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.Serializable;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * Tests the CouchbaseCache class and verifies its functionality.
 *
 * @author Michael Nitschinger
 * @author Konrad Król
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class CouchbaseCacheTests {

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
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    assertEquals(cacheName, cache.getName());
    assertEquals(client, cache.getNativeCache());
  }

  /**
   * Verifies set() and get() of cache objects.
   */
  @Test
  public void testGetSet() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    String value = "Hello World!";
    cache.put(key, value);

    String stored = cache.get(key, String.class);
    assertNotNull(stored);
    assertEquals(value, stored);

    ValueWrapper loaded = cache.get(key);
    assertNotNull(loaded);
    assertEquals(value, loaded.get());
  }

  /**
   * Verifies set() with TTL value.
   */
  @Test
  public void testSetWithTtl() throws InterruptedException {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client, 1); // cache for 1 second

    String key = "couchbase-cache-test";
    String value = "Hello World!";
    cache.put(key, value);

    // wait for TTL to expire (double time of TTL)
    Thread.sleep(2000);

    String stored = cache.get(key, String.class);
    assertNull(stored);
  }

  @Test
  public void testGetSetWithCast() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-user";
    User user = new User();
    user.firstname = "Michael";

    cache.put(key, user);

    User loaded = cache.get(key, User.class);
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
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    String value = "Hello World!";

    cache.put(key, value);
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
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    String value = "Hello World!";

    cache.put(key, value);
    cache.put(key, null);

    assertNull(cache.get(key));
  }

  /**
   * Putting into cache on the same key not null value and then clearing the cache,
   * results in null object
   */
  @Test
  public void testClearingUsingViewDoesntImpactUnrelatedDocuments() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);
    assertFalse("Expected flush to be disabled by default", cache.getAlwaysFlush());

    String key = "couchbase-cache-test";
    String unrelatedId = "randomKey";
    String value = "Hello World!";

    cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
    cache.put(key, value);
    cache.clear();

    assertNotNull(cache.getNativeCache().remove(unrelatedId));
    assertNull(cache.get(key));
  }

  @Test
  public void testClearingEmptyNameCacheUsingViewDoesntImpactUnrelatedDocuments() {
    CouchbaseCache cache = new CouchbaseCache(null, client);
    assertFalse("Expected flush to be disabled by default", cache.getAlwaysFlush());

    String key = "couchbase-cache-test";
    String unrelatedId = "randomKey:in:parts";
    String value = "Hello World!";

    cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
    cache.put(key, value);
    cache.clear();

    assertNotNull(cache.getNativeCache().remove(unrelatedId));
    assertNull(cache.get(key));
  }

  /**
   * Putting into cache on the same key not null value and then clearing the cache,
   * results in null object
   */
  @Test
  public void testClearingUsingFlushImpactsUnrelatedDocuments() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);
    cache.setAlwaysFlush(true);

    String key = "couchbase-cache-test";
    String unrelatedId = "randomKey";
    String value = "Hello World!";

    cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));
    cache.put(key, value);
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
    CouchbaseCache cache = new CouchbaseCache("emptyCache", client);
    String unrelatedId = "unrelated";
    cache.getNativeCache().upsert(JsonDocument.create(unrelatedId, JsonObject.empty()));

    try {
      cache.clear();
    } catch (NoSuchElementException e) {
      fail("Cache clearing failed on empty cache: " + e.toString());
    }
    assertTrue(cache.getNativeCache().exists(unrelatedId));
  }

  static class User implements Serializable {
    public String firstname;
  }

}
