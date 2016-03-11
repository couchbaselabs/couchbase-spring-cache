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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.java.Bucket;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.util.CollectionUtils;

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

  private final Bucket defaultBucket;

  private boolean dynamic = true;

  private int defaultTtl = 0;

  private List<Config> staticCaches = new LinkedList<Config>();

  /**
   * Construct a CacheManager backed by a default {@link Bucket} and knowing about
   * a predetermined set of caches at construction.
   *
   * @param defaultBucket the backing bucket to use for all caches.
   * @param cacheNames the names of caches recognized by this manager. A null or empty value
   * would make this manager dynamic instead.
   */
  public CouchbaseCacheManager(Bucket defaultBucket, Collection<String> cacheNames) {
    this.defaultBucket = defaultBucket;
    Set<String> names = CollectionUtils.isEmpty(cacheNames) ? Collections.<String> emptySet()
            : new HashSet<String>(cacheNames);
    this.dynamic = names.isEmpty();
    for (String name : names) {
      staticCaches.add(new Config(name, defaultBucket, defaultTtl));
    }
  }

  /**
   * Construct a CacheManager backed by a default {@link Bucket} and knowing about
   * a predetermined set of caches at construction, all of which share the same expiration TTL.
   *
   * @param defaultBucket the backing bucket to use for all caches.
   * @param ttl the expiration TTL for all the declared caches.
   * @param cacheNames the names of caches recognized by this manager. A null or empty value
   * would make this manager dynamic instead.
   */
  public CouchbaseCacheManager(Bucket defaultBucket, int ttl, Collection<String> cacheNames) {
    this.defaultBucket = defaultBucket;
    this.defaultTtl = ttl;
    Set<String> names = CollectionUtils.isEmpty(cacheNames) ? Collections.<String> emptySet()
            : new HashSet<String>(cacheNames);
    this.dynamic = names.isEmpty();
    for (String name : names) {
      staticCaches.add(new Config(name, defaultBucket, ttl));
    }
  }

  /**
   * Construct a CacheManager backed by a default {@link Bucket} and knowing about
   * a predetermined set of caches at construction, each with different ttl.
   *
   * @param defaultBucket the backing bucket to use for all caches.
   * @param cacheNamesAndTtls a map of the names of caches recognized by this manager and their individual ttl setting.
   * A null or empty value would make this manager dynamic instead.
   */
  public CouchbaseCacheManager(Bucket defaultBucket, Map<String, Integer> cacheNamesAndTtls) {
    this.defaultBucket = defaultBucket;
    Map<String, Integer> namesAndTtl = CollectionUtils.isEmpty(cacheNamesAndTtls) ? Collections.<String, Integer> emptyMap()
            : cacheNamesAndTtls;
    this.dynamic = namesAndTtl.isEmpty();
    for (Map.Entry<String, Integer> nameAndTtl : namesAndTtl.entrySet()) {
      String name = nameAndTtl.getKey();
      Integer ttl = nameAndTtl.getValue();
      staticCaches.add(new Config(name, defaultBucket, ttl));
    }
  }

  /**
   * Construct a CacheManager knowing about a predetermined set of caches at construction, as described
   * by at list one {@link Config} (cache name, backing {@link Bucket} and ttl).
   *
   * At least one cache is required (this constructor doesn't allow for dynamic creation of caches, see
   * other constructors for that).
   *
   * @param firstCache the first cache {@link Config specification}.
   * @param otherStaticCaches optionally empty vararg of additional cache specifications.
   */
  public CouchbaseCacheManager(Config firstCache, Config... otherStaticCaches) {
    if (firstCache == null) {
      throw new NullPointerException("At least one cache specification is required");
    }
    this.defaultBucket = firstCache.cacheClient;
    this.dynamic = false;

    staticCaches.add(firstCache);
    if (otherStaticCaches != null) {
      Collections.addAll(staticCaches, otherStaticCaches);
    }
  }

  /**
   * Construct a CacheManager backed by a default {@link Bucket} and capable of dynamically adding
   * new caches as they are requested.
   *
   * @param defaultBucket the backing bucket to use for all dynamic caches.
   */
  public CouchbaseCacheManager(Bucket defaultBucket) {
    this(defaultBucket, Collections.<String>emptyList());
  }

  /**
   * Set the default Time To Live value for all caches created from that point
   * forward.
   */
  public void setDefaultTtl(int defaultTtl) {
    this.defaultTtl = defaultTtl;
  }

  /**
   * Dynamically and forcibly add a new cache backed by the default bucket.
   *
   * @param cacheName the name of the cache to add.
   * @param ttl the ttl/expiry for elements in the new cache.
   */
  public void addCache(String cacheName, int ttl) {
    addCache(cacheName, ttl, defaultBucket);
  }

  /**
   * Dynamically and foribly add a new cache backed by a specific bucket.
   *
   * @param cacheName the name of the cache to add.
   * @param ttl the ttl/expiry for elements in the new cache.
   * @param bucket the {@link Bucket} backing the new cache.
   */
  public void addCache(String cacheName, int ttl, Bucket bucket) {
    addCache(createCache(cacheName, ttl, bucket));
  }

  @Override
  protected Cache getMissingCache(String name) {
    if (this.dynamic) {
      return createCache(name, defaultTtl, defaultBucket);
    }
    return null;
  }

  /**
   * Populates all caches.
   *
   * @return a collection of loaded caches.
   */
  @Override
  protected final Collection<? extends Cache> loadCaches() {
    Collection<Cache> caches = new LinkedHashSet<Cache>();

    for (Config cfg : staticCaches) {
      int ttl = defaultTtl;
      if (cfg.cacheTtl != null) {
        ttl = cfg.cacheTtl;
      }
      caches.add(createCache(cfg.cacheName, ttl, cfg.cacheClient));
    }

    return caches;
  }

  private CouchbaseCache createCache(String name, Integer ttl, Bucket bucket) {
    return new CouchbaseCache(name, bucket, ttl);
  }

  /**
   * A class that represents information about a {@link CouchbaseCache} to be added to the {@link CouchbaseCacheManager}
   * at construction time (statically).
   */
  public static class Config {
    final public String cacheName;
    final public Bucket cacheClient;
    final public Integer cacheTtl;

    /**
     * Specifies the information necessary to further construction of a CouchbaseCache.
     *
     * @param cacheName the mandatory name for the cache.
     * @param cacheClient the mandatory backing bucket for the cache.
     * @param cacheTtl the optional ttl for the cache. If null, the manager's default ttl will be used.
     */
    public Config(String cacheName, Bucket cacheClient, Integer cacheTtl) {
      if (cacheName == null || cacheClient == null) {
        throw new NullPointerException("Cache name and backing client are both mandatory for cache construction");
      }
      this.cacheName = cacheName;
      this.cacheClient = cacheClient;
      this.cacheTtl = cacheTtl;
    }
  }

}
