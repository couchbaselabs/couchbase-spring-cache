package com.couchbase.client.spring.cache;

import com.couchbase.client.java.Bucket;

/**
 * A class that represents a template, or default values, for future creation of caches.
 *
 * @author Simon Baslé
 */
public class CacheTemplate {

  private static final int DEFAULT_TTL = 0;

  protected final Bucket cacheClient;
  protected int cacheExpiry;

  public CacheTemplate(Bucket bucket) {
    if (bucket == null) {
      throw new NullPointerException("A non-null Bucket is required for all cache builders");
    }

    this.cacheClient = bucket;
    this.cacheExpiry = DEFAULT_TTL;
  }

  /**
   * Give a default expiration (or TTL) to the cache to be built.
   *
   * @param expiration the expiration delay in milliseconds.
   * @return this builder for chaining.
   */
  public CacheTemplate withExpirationInMillis(int expiration) {
    this.cacheExpiry = expiration;
    return this;
  }

  public Bucket getDefaultClient() {
    return cacheClient;
  }

  public int getDefaultExpiry() {
    return cacheExpiry;
  }
}
