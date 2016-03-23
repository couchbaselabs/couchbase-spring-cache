# couchbase-spring-cache 2.0.0
An implementation for Spring Cache based on Couchbase Java SDK 2.x

Suitable to work with Spring Data Couchbase 2.0.x and above, as the two projects can share the same backing SDK `Cluster` and `Bucket`s. This is especially awesome for Spring Boot apps (both Spring Data Couchbase and Couchbase Spring Cache are included in Spring Boot 1.4.0).

## Usage
Instantiate a `CouchbaseCacheManager` using `CacheBuilder` to either create several caches sharing the same template or a map of builders to individually customize several preloaded caches.

```java
//preloaded cache manager, same configs
new CouchbaseCacheManager(new CacheBuilder().withBucket(bucket), "cache1", "cache2");

//preloaded cache manager, custom configs
Map<String, CacheBuilder> caches = new HashMap<String, CacheBuilder>();
caches.put("cacheA", new CacheBuilder().withBucket(bucket).withExpirationInMillis(2000));
caches.put("cacheB", new CacheBuilder().withBucket(bucket2).withExpirationInMillis(3000));
new CouchbaseCacheManager(caches);

//dynamic-capable cache manager
//no cache is preloaded but it will create them on demand using the builder as a template
new CouchbaseCacheManager(new CacheBuilder().withBucket(bucket).withExpirationInMillis(1000));
```

Notice how the `CacheBuilder` allows you to describe how the `Cache` is backed by Couchbase by providing a `Bucket` from the Couchbase Java SDK, among other tunings.

Note that for now **only buckets of type "`couchbase`" are supported**, so that several caches can be created over the same `Bucket` and still cleared independently.

## How to get it: how to build
Release 2.0.0 is available on Maven Central. Edit your `pom.xml` and add the following dependency in the `dependencies` section:

```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>couchbase-spring-cache</artifactId>
    <version>2.0.0</version>
</dependency>
```

You can also build the latest snapshot yourself from master:

```bash
git clone https://github.com/couchbaselabs/couchbase-spring-cache.git
cd couchbase-spring-cache
mvn clean install
```

### Background, why a 2.x version?
This was historically part of Spring Data Couchbase up until the 2.0.0 version, which was rewritten to use the Java SDK 2.x.
The project was then extracted so that it could be reworked on top of the same SDK, while not being strongly tied to Spring Data (which can have longer release cycles).

This also allows for a rework of the initial 2.0.0 API, with tight feedback from the Spring Boot project, so that it can be well integrated into Spring Boot.