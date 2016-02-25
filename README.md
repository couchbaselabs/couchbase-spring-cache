# couchbase-spring-cache 2.0.0
An implementation for Spring Cache based on Couchbase Java SDK 2.x

Suitable to work with Spring Data Couchbase 2.0.0, as the two implementation can share the same backing SDK `Cluster` and `Bucket`s.

This is a work in progress towards the first release out of this repo, `2.0.0` (we try to run tests and maintain the master branch runnable, but breaking changes may still be introduced until first GA release).

## How to get it: how to build
Ultimately, release 2.0.0 will be made available through Maven Central. In the meantime and while the API is being reworked, you can build the snapshot yourself from master:

```bash
git clone https://github.com/couchbaselabs/couchbase-spring-cache.git
cd couchbase-spring-cache
mvn clean install
```

### Background, why a 2.x version?
This was historically part of Spring Data Couchbase up until the 2.0.0 version, which was rewritten to use the Java SDK 2.x.
The project was then extracted so that it could be reworked on top of the same SDK, while not being strongly tied to Spring Data (which can have longer release cycles).

This also allows for a rework of the initial 2.0.0 API, with tight feedback from the Spring Boot project, so that it can be well integrated into Spring Boot.