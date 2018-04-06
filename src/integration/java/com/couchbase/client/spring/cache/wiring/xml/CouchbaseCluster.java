package com.couchbase.client.spring.cache.wiring.xml;

import java.util.List;

public class CouchbaseCluster {

  public static final com.couchbase.client.java.CouchbaseCluster create(List<String> nodes, String username, String password) {
    return com.couchbase.client.java.CouchbaseCluster.create(nodes).authenticate(username, password);
  }
}
