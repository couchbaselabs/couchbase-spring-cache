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

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractCacheManager;

/**
 * The {@link CouchbaseCacheManager} orchestrates {@link CouchbaseCache} instances.
 * 
 * Since more than one current {@link Bucket} connection can be used for caching, the
 * {@link CouchbaseCacheManager} orchestrates and handles them for the Spring {@link Cache} abstraction layer.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Konrad Król
 * @author Stéphane Nicoll
 */
public class CouchbaseCacheManager extends AbstractCacheManager {

  private CacheBuilder defaultCacheBuilder;
  private final Collection<CouchbaseCache> initialCaches;

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all prepared using the provided {@link CacheBuilder} (at least specifying a backing {@link Bucket}).
   *
   * Later on, additional caches can be added dynamically just by name, as they'll use the template for
   * configuration as well.
   *
   * @param cacheBuilder the template (backing client, optional ttl) to use for all caches that needs to be
   *  dynamically created.
   * @param cacheNames the names of caches recognized by this manager initially. Caches can be added dynamically later,
   * and null names will be ignored.
   */
  public CouchbaseCacheManager(CacheBuilder cacheBuilder, String... cacheNames) {
    Set<String> names = cacheNames.length == 0? Collections.<String> emptySet()
            : new LinkedHashSet<String>(Arrays.asList(cacheNames));
    this.initialCaches = new ArrayList<CouchbaseCache>();
    for (String name : names) {
      if (name != null) {
        this.initialCaches.add(cacheBuilder.build(name));
      }
    }
    if (this.initialCaches.isEmpty()) {
      this.defaultCacheBuilder = cacheBuilder;
    }
  }

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all explicitly described (using a {@link CacheBuilder}.
   *
   * @param initialCaches the caches to make available on startup
   */
  public CouchbaseCacheManager(Map<String, CacheBuilder> initialCaches) {
    if (initialCaches == null || initialCaches.isEmpty()) {
      throw new IllegalArgumentException("At least one cache builder must be specified.");
    }
    this.initialCaches = new ArrayList<CouchbaseCache>();
    for (Map.Entry<String, CacheBuilder> entry : initialCaches.entrySet()) {
      this.initialCaches.add(entry.getValue().build(entry.getKey()));
    }
  }

  /**
   * Set the default cache builder to use to create caches on the fly. Set it to {@code null}
   * to prevent cache to be created at runtime.
   * @param defaultCacheBuilder the cache builder to use
   */
  public void setDefaultCacheBuilder(CacheBuilder defaultCacheBuilder) {
    this.defaultCacheBuilder = defaultCacheBuilder;
  }

  @Override
  protected Cache getMissingCache(String name) {
    return (defaultCacheBuilder != null ? defaultCacheBuilder.build(name) : null);
  }

  @Override
  protected final Collection<? extends Cache> loadCaches() {
    return this.initialCaches;
  }

}
