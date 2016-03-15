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
import java.util.Set;

import com.couchbase.client.java.Bucket;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
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

  private final CacheTemplate template;
  private List<CacheBuilder> staticCaches = new LinkedList<CacheBuilder>();

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all prepared using the provided {@link CacheTemplate} (at least specifying a backing {@link Bucket}).
   *
   * Later on, additional caches can be added dynamically just by name, as they'll use the template for
   * configuration as well.
   *
   * @param dynamicTemplate the template (backing client, optional ttl) to use for all caches that needs to be
   *  dynamically created.
   * @param cacheNames the names of caches recognized by this manager initially. Caches can be added dynamically later,
   * and null names will be ignored.
   */
  public CouchbaseCacheManager(CacheTemplate dynamicTemplate, Collection<String> cacheNames) {
    this.template = dynamicTemplate;
    Set<String> names = CollectionUtils.isEmpty(cacheNames) ? Collections.<String> emptySet()
            : new HashSet<String>(cacheNames);
    for (String name : names) {
      if (name != null) {
        staticCaches.add(new CacheBuilder(name, dynamicTemplate));
      }
    }
  }

  /**
   * Construct a {@link CacheManager} knowing about a predetermined set of caches at construction. The caches are
   * all explicitly described (using a {@link CacheBuilder}. In order to later be able to simply add a dynamic cache
   * just by name, a {@link CacheTemplate} should be provided. This will enable on-demand creation of new caches when
   * an unknown name is requested.
   *
   * @param dynamicTemplate the template (backing client, optional ttl) to use for dynamically creating a cache later on.
   * @param preloadedCaches the builders of caches recognized by this manager initially (nulls are ignored).
   */
  public CouchbaseCacheManager(CacheTemplate dynamicTemplate, CacheBuilder... preloadedCaches) {
    this.template = dynamicTemplate;
    if (preloadedCaches != null) {
      for (CacheBuilder pcache : preloadedCaches) {
        if (pcache != null) {
          staticCaches.add(pcache);
        }
      }
    }
  }

  @Override
  protected Cache getMissingCache(String name) {
    if (template == null) {
      return null; //dynamically adding by name isn't possible, no "template"
    } else {
      CacheBuilder builder = new CacheBuilder(name, template);
      return builder.build();
    }
  }

  /**
   * Populates all caches.
   *
   * @return a collection of loaded caches.
   */
  @Override
  protected final Collection<? extends Cache> loadCaches() {
    Collection<Cache> caches = new LinkedHashSet<Cache>();

    for (CacheBuilder cfg : staticCaches) {
      caches.add(cfg.build());
    }

    return caches;
  }

  /**
   * Programmatically add a cache to the manager, using an explicit {@link CacheBuilder}. This works in any
   * configuration.
   *
   * @param cacheBuilder the builder explicitly describing the cache to add.
   */
  public void addCache(CacheBuilder cacheBuilder) {
    addCache(cacheBuilder.build());
  }

  /**
   * Programmatically add a cache to the manager using the {@link CacheTemplate} that was passed in at construction
   * time. This is the same as requesting an unknown cache by name (as the later will also use the template to
   * dynamically create a new cache).
   * @param name
   */
  public void addCache(String name) {
    if (template == null) {
      throw new IllegalStateException("Dynamic adding of a cache with just a name requires a cache template at construction");
    }
    addCache(new CacheBuilder(name, template));
  }
}
