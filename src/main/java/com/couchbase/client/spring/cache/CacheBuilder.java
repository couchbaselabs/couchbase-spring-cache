package com.couchbase.client.spring.cache;

import com.couchbase.client.java.Bucket;

/**
 * A builder for a {@link CouchbaseCache}, enforcing at a minimum a name and a backing {@link Bucket}.
 *
 * @author Simon Baslé
 */
public class CacheBuilder extends CacheTemplate {

  private final String cacheName;

  /**
   * Start a new {@link CacheBuilder}, specifying the name of the cache to be created
   * and its backing {@link Bucket}.
   *
   * @param name the name for the cache to be built.
   * @param bucket the backing bucket for the cache.
   */
  public CacheBuilder(String name, Bucket bucket) {
    super(bucket);
    this.cacheName = name;
  }

  /**
   * Start a new {@link CacheBuilder} from an existing {@link CacheTemplate}.
   *
   * @param name the name for the cache to be built.
   * @param template the template from which to get the backing bucket and expiration.
   */
  public CacheBuilder(String name, CacheTemplate template) {
    super(template.getDefaultClient());
    this.cacheName = name;
    this.cacheExpiry = template.getDefaultExpiry();
  }

  @Override
  public CacheBuilder withExpirationInMillis(int expiration) {
    super.withExpirationInMillis(expiration);
    return this;
  }

  /**
   * Build the {@link CouchbaseCache} from the configuration embedded by this builder.
   *
   * @return a new CouchbaseCache.
   */
  public CouchbaseCache build() {
    if (cacheName == null || cacheClient == null) {
      throw new NullPointerException("Cache name and backing client are both mandatory for cache construction");
    }
    return new CouchbaseCache(cacheName, cacheClient, cacheExpiry);
  }

}
