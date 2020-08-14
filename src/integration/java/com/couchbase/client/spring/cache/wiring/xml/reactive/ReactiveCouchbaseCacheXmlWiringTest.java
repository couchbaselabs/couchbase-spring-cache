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
package com.couchbase.client.spring.cache.wiring.xml.reactive;

import com.couchbase.client.spring.cache.wiring.CachedService;
import com.couchbase.client.spring.cache.wiring.reactive.ReactiveAbstractCouchbaseCacheWiringTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test case for the xml driven configuration, wiring and execution of a {@link Cacheable}-annotated
 * {@link CachedService}.
 *
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "reactive-test-manager-context.xml"})
public class ReactiveCouchbaseCacheXmlWiringTest extends ReactiveAbstractCouchbaseCacheWiringTest {

  @Test
  public void testBeans() {
    assertNotNull(cluster);
    assertNotNull(bucket);
    assertEquals("default", bucket.name());
    assertNotNull(defaultBuilder);
    assertEquals(bucket, defaultBuilder.build("test").getNativeCache());
    assertNotNull(cacheManager);

    Set<String> expectedCaches = new HashSet<String>(3);
    expectedCaches.add("staticCache1");
    expectedCaches.add("staticCache2");
    expectedCaches.add("staticCache3");
    expectedCaches.add("dataCache");
    assertEquals(expectedCaches, cacheManager.getCacheNames());
  }

}
