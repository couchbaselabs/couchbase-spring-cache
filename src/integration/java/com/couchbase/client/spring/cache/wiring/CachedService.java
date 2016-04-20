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
package com.couchbase.client.spring.cache.wiring;

import java.io.Serializable;

import com.couchbase.client.spring.cache.CouchbaseCache;
import com.couchbase.client.spring.cache.wiring.javaConfig.CacheEnabledTestConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

/**
 * A test Service with {@link Cacheable} methods to demo and test the wiring of a {@link CouchbaseCache}.
 *
 * @author Simon Baslé
 */
@Repository
public class CachedService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachedService.class);

  private long counterGetData = 0L;
  private long counterGetDataWithSize = 0L;
  private long counterGetDataWrongCache = 0L;

  @Cacheable(CacheEnabledTestConfiguration.DATA_CACHE_NAME)
  public Data getData(String criteriaA, String criteriaB) {
    counterGetData++;
    LOGGER.info("Calling getData {} {} ", criteriaA, criteriaB);
    return new Data(criteriaA, criteriaB, criteriaA.length());
  }

  @Cacheable(value = CacheEnabledTestConfiguration.DATA_CACHE_NAME,
    key = "#criteriaA", unless = "#size < 5")
  public Data getDataWithSize(String criteriaA, String criteriaB, int size) {
    counterGetDataWithSize++;
    LOGGER.info("Calling getDataWithSize {} {} {}", criteriaA, criteriaB, size);
    return new Data(criteriaA, criteriaB, size);
  }

  @CacheEvict(value = CacheEnabledTestConfiguration.DATA_CACHE_NAME,
    key = "#data.valueA")
  public void store(Data data) {
    LOGGER.info("Persisted {} to disk", data);
  }

  @Cacheable("wrongCache")
  public Data getDataWrongCache(String criteria) {
    counterGetDataWrongCache++;
    LOGGER.info("Calling getDataWrongCache {} ", criteria);
    return new Data(criteria, criteria.toUpperCase(), 10);
  }

  public long getCounterGetData() {
    return counterGetData;
  }

  public long getCounterGetDataWithSize() {
    return counterGetDataWithSize;
  }

  public long getCounterGetDataWrongCache() {
    return counterGetDataWrongCache;
  }

  public void resetCounters() {
    this.counterGetData = 0L;
    this.counterGetDataWithSize = 0L;
    this.counterGetDataWrongCache= 0L;
  }

  public static class Data implements Serializable {
    private static final long serialVersionUID = 101106066653013623L;

    private String valueA;
    private String valueB;
    private int intValue;

    public Data(String valueA, String valueB, int intValue) {
      this.valueA = valueA;
      this.valueB = valueB;
      this.intValue = intValue;
    }

    public String getValueA() {
      return valueA;
    }

    public String getValueB() {
      return valueB;
    }

    public int getIntValue() {
      return intValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Data data = (Data) o;

      if (intValue != data.intValue) return false;
      if (valueA != null ? !valueA.equals(data.valueA) : data.valueA != null) return false;
      return !(valueB != null ? !valueB.equals(data.valueB) : data.valueB != null);

    }

    @Override
    public int hashCode() {
      int result = valueA != null ? valueA.hashCode() : 0;
      result = 31 * result + (valueB != null ? valueB.hashCode() : 0);
      result = 31 * result + intValue;
      return result;
    }

    @Override
    public String toString() {
      return "Data{" +
          "valueA='" + valueA + '\'' +
          ", valueB='" + valueB + '\'' +
          ", intValue=" + intValue +
          '}';
    }
  }
}
