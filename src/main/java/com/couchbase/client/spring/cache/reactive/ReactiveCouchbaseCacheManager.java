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
import com.couchbase.client.spring.cache.CouchbaseCache;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link ReactiveCouchbaseCacheManager} orchestrates {@link CouchbaseCache} instances.
 * 
 * Since more than one current {@link Bucket} connection can be used for caching, the
 * {@link ReactiveCouchbaseCacheManager} orchestrates and handles them for the Spring {@link Cache} abstraction layer.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Konrad Król
 * @author Stéphane Nicoll
 */
public class ReactiveCouchbaseCacheManager extends AbstractCacheManager {

  private ReactiveCacheBuilder defaultCacheBuilder;

  private boolean initialized;
  private final Map<String, ReactiveCacheBuilder> initialCaches;

  /**
   * Construct a {@link CacheManager} with a "template" {@link ReactiveCacheBuilder} (at least specifying a backing
   * {@link Bucket}).
   *
   * If a list of predetermined cache names is provided, the manager is "static" and these caches will all be
   * prepared using the provided template builder.
   *
   * If no list is provided, the manager will be "dynamic" and additional caches can be added later on just by name,
   * as they'll use the template builder for their configuration.
   *
   * Note that builders are used lazily and should not be mutated after having been passed to this constructor.
   *
   * @param cacheBuilder the template (backing client, optional ttl) to use either for static construction of specified
   * caches or later dynamic construction.
   * @param cacheNames the names of caches recognized by this manager initially. If empty, caches can be added
   * dynamically later. Null names will be ignored.
   * @see ReactiveCouchbaseCacheManager#setDefaultCacheBuilder(ReactiveCacheBuilder) to force activation of dynamic creation later on.
   */
  public ReactiveCouchbaseCacheManager(ReactiveCacheBuilder cacheBuilder, String... cacheNames) {
    if (cacheBuilder == null) {
      throw new NullPointerException("ReactiveCacheBuilder template is mandatory");
    }
    Set<String> names = cacheNames.length == 0? Collections.<String> emptySet()
            : new LinkedHashSet<String>(Arrays.asList(cacheNames));
    this.initialCaches = new HashMap<String, ReactiveCacheBuilder>(names.size());
    for (String name : names) {
      if (name != null) {
        this.initialCaches.put(name, cacheBuilder);
      }
    }
    if (this.initialCaches.isEmpty()) {
      this.defaultCacheBuilder = cacheBuilder;
    }
  }

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all explicitly described (using a {@link ReactiveCacheBuilder} and the manager cannot create caches dynamically until
   * {@link #setDefaultCacheBuilder(ReactiveCacheBuilder)} is called.
   *
   * Note that builders are used lazily and should not be mutated after having been passed to this constructor.
   *
   * @param initialCaches the caches to make available on startup
   */
  public ReactiveCouchbaseCacheManager(Map<String, ReactiveCacheBuilder> initialCaches) {
    if (initialCaches == null || initialCaches.isEmpty()) {
      throw new IllegalArgumentException("At least one cache builder must be specified.");
    }
    this.initialCaches = new HashMap<String, ReactiveCacheBuilder>(initialCaches);
  }

  /**
   * Set the default cache builder to use to create caches on the fly. Set it to {@code null}
   * to prevent cache to be created at runtime.
   *
   * @param defaultCacheBuilder the cache builder to use
   */
  public void setDefaultCacheBuilder(ReactiveCacheBuilder defaultCacheBuilder) {
    this.defaultCacheBuilder = defaultCacheBuilder;
  }

  /**
   * Register an additional cache with the specified name using the default builder.
   * <p>
   * Caches can only be configured at initialization time. Once the cache manager
   * has been initialized, no cache can be further prepared.
   * @param name the name of the cache to add
   * @throws IllegalStateException if no builder is available or if the cache manager
   * has already been initialized
   */
  public void prepareCache(String name) {
    if (defaultCacheBuilder == null) {
      throw new IllegalStateException("No default cache builder is specified.");
    }
    prepareCache(name, defaultCacheBuilder);
  }

  /**
   * Register an additional cache with the specified name using the specified
   * {@link ReactiveCacheBuilder}.
   * <p>
   * Caches can only be configured at initialization time. Once the cache manager
   * has been initialized, no cache can be further prepared.
   * @param name the name of the cache to add
   * @param builder builder
   * @throws IllegalStateException the cache manager has already been initialized
   */
  public void prepareCache(String name, ReactiveCacheBuilder builder) {
    if (initialized) {
      throw new IllegalStateException("This cache manager has already been initialized. No " +
              "further cache can be prepared.");
    }
    this.initialCaches.put(name, builder);
  }

  @Override
  protected Cache getMissingCache(String name) {
    return (defaultCacheBuilder != null ? defaultCacheBuilder.build(name) : null);
  }

  @Override
  protected final Collection<? extends Cache> loadCaches() {
    initialized = true;
    List<Cache> caches = new LinkedList<Cache>();
    for (Map.Entry<String, ReactiveCacheBuilder> entry : initialCaches.entrySet()) {
      caches.add(entry.getValue().build(entry.getKey()));
    }
    return caches;
  }

}
