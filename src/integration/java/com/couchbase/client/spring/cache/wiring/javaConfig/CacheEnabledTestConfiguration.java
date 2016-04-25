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
package com.couchbase.client.spring.cache.wiring.javaConfig;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.spring.cache.CacheBuilder;
import com.couchbase.client.spring.cache.CouchbaseCacheManager;
import com.couchbase.client.spring.cache.TestConfiguration;
import com.couchbase.client.spring.cache.wiring.CachedService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * A test {@link Configuration} that scans for services in the same package and enables caching on them.
 * Recognizes a single cache named "dataCache".
 *
 * @author Simon Baslé
 */
@EnableCaching
@Configuration
@Import(TestConfiguration.class)
@ComponentScan(basePackageClasses = CachedService.class)
public class CacheEnabledTestConfiguration {

  public static final String DATA_CACHE_NAME = "dataCache";

  @Bean
  public CacheBuilder defaultBuilder(final Bucket bucket) {
    return CacheBuilder.newInstance(bucket);
  }

  @Bean
  public CacheManager cacheManager(final Bucket bucket) {
    return new CouchbaseCacheManager(defaultBuilder(bucket), DATA_CACHE_NAME);
  }
}
