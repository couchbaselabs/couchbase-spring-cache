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

import static java.util.stream.Collectors.toSet;

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

  private boolean initialized;
  private final Map<String, CacheBuilder> initialCaches;
  private boolean enabled = true;

  /**
   * Construct a {@link CacheManager} with a "template" {@link CacheBuilder} (at least specifying a backing
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
   * @param enabled The initial value of {@link #setAllEnabled(boolean)}
   * @param cacheNames the names of caches recognized by this manager initially. If empty, caches can be added
   * dynamically later. Null names will be ignored.
   * @see CouchbaseCacheManager#setDefaultCacheBuilder(CacheBuilder) to force activation of dynamic creation later on.
   */
  public CouchbaseCacheManager(CacheBuilder cacheBuilder, boolean enabled, String... cacheNames) {
    if (cacheBuilder == null) {
      throw new NullPointerException("CacheBuilder template is mandatory");
    }
    Set<String> names = cacheNames.length == 0? Collections.<String> emptySet()
            : new LinkedHashSet<String>(Arrays.asList(cacheNames));
    this.initialCaches = new HashMap<String, CacheBuilder>(names.size());
    for (String name : names) {
      if (name != null) {
        this.initialCaches.put(name, cacheBuilder);
      }
    }
    if (this.initialCaches.isEmpty()) {
      this.defaultCacheBuilder = cacheBuilder;
    }
    // Individual caches to be set after loadCaches()
    this.enabled = enabled;
  }

  public CouchbaseCacheManager(CacheBuilder cacheBuilder, String... cacheNames) {
    this(cacheBuilder, true, cacheNames);
  }

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all explicitly described (using a {@link CacheBuilder} and the manager cannot create caches dynamically until
   * {@link #setDefaultCacheBuilder(CacheBuilder)} is called.
   *
   * Note that builders are used lazily and should not be mutated after having been passed to this constructor.
   *
   * @param initialCaches the caches to make available on startup
   */
  public CouchbaseCacheManager(Map<String, CacheBuilder> initialCaches) {
    if (initialCaches == null || initialCaches.isEmpty()) {
      throw new IllegalArgumentException("At least one cache builder must be specified.");
    }
    this.initialCaches = new HashMap<String, CacheBuilder>(initialCaches);
  }

  /**
   * Set the default cache builder to use to create caches on the fly. Set it to {@code null}
   * to prevent cache to be created at runtime.
   *
   * @param defaultCacheBuilder the cache builder to use
   */
  public void setDefaultCacheBuilder(CacheBuilder defaultCacheBuilder) {
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
   * {@link CacheBuilder}.
   * <p>
   * Caches can only be configured at initialization time. Once the cache manager
   * has been initialized, no cache can be further prepared.
   * @param name the name of the cache to add
   * @param builder CacheBuildercp
   * @throws IllegalStateException the cache manager has already been initialized
   */
  public void prepareCache(String name, CacheBuilder builder) {
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
    for (Map.Entry<String, CacheBuilder> entry : initialCaches.entrySet()) {
      caches.add(entry.getValue().build(entry.getKey()));
    }
    setAllEnabled(this.enabled);
    return caches;
  }

  public void clearAll() {
    this.getCacheNames().forEach(name -> {
      this.getCache(name).clear();
    });
  }

  private Set<EnableableCache> getEnableableCaches() {
    return this.getCacheNames()
            .stream()
            .map(this::getCache)
            .filter(cache -> cache instanceof EnableableCache)
            .map(cache -> (EnableableCache) cache)
            .collect(toSet());
  }

  public void setAllEnabled(boolean enabled) {
    this.enabled = enabled;
    getEnableableCaches()
            .forEach(cache -> cache.setEnabled(enabled));
  }

  public boolean areAllEnabled() {
    //Anything that isn't an EnableableCache is automatically considered enabled.
    return getEnableableCaches()
            .stream()
            .allMatch(EnableableCache::isEnabled);
  }

}
