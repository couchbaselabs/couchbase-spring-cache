package com.couchbase.client.spring.cache;

import com.couchbase.client.java.Bucket;

/**
 * A builder for {@link CouchbaseCache} instance.
 *
 * @author Simon Baslé
 */
public class CacheBuilder {

  private static final int DEFAULT_TTL = 0;

  private Bucket bucket;
  private int cacheExpiry;

  protected CacheBuilder() {
    this.cacheExpiry = DEFAULT_TTL;
  }

  /**
   * Create a new builder instance with the given {@link Bucket}.
   * @param bucket the bucket to use
   * @return a new builder
   */
  public static CacheBuilder newInstance(Bucket bucket) {
    return new CacheBuilder().withBucket(bucket);
  }

  /**
   * Give a bucket to the cache to be built.
   * @param bucket the bucket
   * @return this builder for chaining.
   */
  public CacheBuilder withBucket(Bucket bucket) {
    if (bucket == null) {
      throw new NullPointerException("A non-null Bucket is required for all cache builders");
    }
    this.bucket = bucket;
    return this;
  }

  /**
   * Give a default expiration (or TTL) to the cache to be built.
   *
   * @param expiration the expiration delay in milliseconds.
   * @return this builder for chaining.
   */
  public CacheBuilder withExpirationInMillis(int expiration) {
    this.cacheExpiry = expiration;
    return this;
  }

  /**
   * Build a new {@link CouchbaseCache} with the specified name.
   * @param cacheName the name of the cache
   * @return a {@link CouchbaseCache} instance
   */
  public CouchbaseCache build(String cacheName) {
    return new CouchbaseCache(cacheName, this.bucket, this.cacheExpiry);
  }

}
