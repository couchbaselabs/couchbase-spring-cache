# couchbase-spring-cache 2.0.0
An implementation for Spring Cache based on Couchbase Java SDK 2.x

Suitable to work with Spring Data Couchbase 2.0.x and above, as the two projects can share the same backing SDK `Cluster` and `Bucket`s. This is especially awesome for Spring Boot apps (both Spring Data Couchbase and Couchbase Spring Cache are included in Spring Boot 1.4.0).

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