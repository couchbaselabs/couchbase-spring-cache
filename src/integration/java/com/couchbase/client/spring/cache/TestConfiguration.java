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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Configuration for basic integration tests.
 *
 * @author Simon Baslé
 */
@Configuration
public class TestConfiguration {

  public String seedNode() {
    return System.getProperty("couchbase.seedNode", "127.0.0.1");
  }

  public String bucketName() {
    return System.getProperty("couchbase.bucketName", "default");
  }
  public String bucketPassword() {
    return System.getProperty("couchbase.bucketPassword", "");
  }

  @Bean(destroyMethod = "disconnect")
  public Cluster cluster() {
    return CouchbaseCluster.create(seedNode());
  }

  @Bean(destroyMethod = "close")
  public Bucket bucket() {
    return cluster().openBucket(bucketName(), bucketPassword());
  }

}
