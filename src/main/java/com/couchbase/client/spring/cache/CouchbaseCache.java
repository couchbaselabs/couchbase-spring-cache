/*
 * Copyright 2013 the original author or authors.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

/**
 * The {@link CouchbaseCache} class implements the Spring {@link Cache} interface on top of Couchbase Server and the
 * Couchbase Java SDK 2.x. Note that data persisted by this Cache should be {@link Serializable}.
 *
 * @see <a href="http://static.springsource.org/spring/docs/current/spring-framework-reference/html/cache.html">
 *   Official Spring Cache Reference</a>
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Konrad Król
 */
public class CouchbaseCache implements Cache {

  private final Logger logger = LoggerFactory.getLogger(CouchbaseCache.class);
  /**
   * The actual SDK {@link Bucket} instance.
   */
  private final Bucket client;

  /**
   * The name of the cache.
   */
  private final String name;
  
  /**
   * TTL value for objects in this cache
   */
  private final int ttl;

  /**
   * Delimiter for separating the name from the key in the document id of objects in this cache
   */
  private final String DELIMITER = ":";

  /**
   * Prefix for identifying all keys relative to {@link CouchbaseCache}s. Given a {@link #DELIMITER} of ':', such keys
   * are in the form <code>CACHE_PREFIX:CACHE_NAME:key</code>. If the cache doesn't have a name it'll be
   * <code>CACHE_PREFIX::key</code>.
   */
  private final String CACHE_PREFIX = "cache";

  /**
   * The design document of the view used by this cache to retrieve documents in a specific namespace.
   */
  private final String CACHE_DESIGN_DOCUMENT = "cache";

  /**
   * The name of the view used by this cache to retrieve documents in a specific namespace.
   */
  private final String CACHE_VIEW = "names";

  /**
   * Determines whether to always use the flush() method to clear the cache.
   */
  private Boolean alwaysFlush = false;

  /**
   * Construct the cache and pass in the {@link Bucket} instance.
   *
   * @param name the name of the cache reference.
   * @param client the Bucket instance.
   */
  public CouchbaseCache(final String name, final Bucket client) {
    this.name = name;
    this.client = client;
    this.ttl = 0;

    if(!getAlwaysFlush())
      ensureViewExists();
  }

  /**
   * Construct the cache and pass in the {@link Bucket} instance.
   *
   * @param name the name of the cache reference.
   * @param client the Bucket instance.
   * @param ttl TTL value for objects in this cache
   */
  public CouchbaseCache(final String name, final Bucket client, int ttl) {
    this.name = name;
    this.client = client;
    this.ttl = ttl;

    if(!getAlwaysFlush())
      ensureViewExists();
  }

  /**
   * Returns the name of the cache.
   *
   * @return the name of the cache.
   */
  @Override
  public final String getName() {
    return name;
  }

  /**
   * Returns the underlying SDK {@link Bucket} instance.
   *
   * @return the underlying Bucket instance.
   */
  @Override
  public final Bucket getNativeCache() {
    return client;
  }
  
  /**
   * Returns the TTL value for this cache.
   * 
   * @return TTL value
   */
  public final int getTtl() {
	  return ttl;
  }

  @Override
  public final ValueWrapper get(final Object key) {
    String documentId = getDocumentId(key.toString());
    SerializableDocument doc = client.get(documentId, SerializableDocument.class);
    if (doc == null) {
      return null;
    }

    //if doc.content is null, the Wrapper is expected to contain null
    return new SimpleValueWrapper(doc.content());
  }

  /**
   * {@inheritDoc}
   * <p>
   * The <code>clazz</code> is expected to be implementing {@link Serializable}.
   */
  @SuppressWarnings("unchecked")
  @Override
  public final <T> T get(final Object key, final Class<T> clazz) {
    String documentId = getDocumentId(key.toString());
    SerializableDocument doc = client.get(documentId, SerializableDocument.class);

    return (doc == null) ? null : (T) doc.content();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Values are expected to be {@link Serializable}.
   *
   * @param key the Key of the storable object.
   * @param value the Serializable object to store.
   */
  @Override
  public final void put(final Object key, final Object value) {
    if (value != null) {
      if (!(value instanceof Serializable)) {
        throw new IllegalArgumentException(String.format("Value %s of type %s is not Serializable", value.toString(), value.getClass().getName()));
      }
      String documentId = getDocumentId(key.toString());
      SerializableDocument doc = SerializableDocument.create(documentId, ttl, (Serializable) value);
      client.upsert(doc);
    } else {
      evict(key);
    }
  }

  @Override
  public final void evict(final Object key) {
    String documentId = getDocumentId(key.toString());
    client.remove(documentId);
  }

  /**
   * Clear the complete cache.
   *
   * Note that this action is very destructive, so only use it with care.
   * Also note that "flush" may not be enabled on the bucket.
   *
   * @see #setAlwaysFlush(Boolean)
   */
  @Override
  public final void clear() {
    if(getAlwaysFlush())
      try {
        client.bucketManager().flush();
      } catch (Exception e) {
        logger.error("Couchbase flush error: ", e);
      }
    else
      evictAllDocuments();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Note that the atomicity in this Couchbase implementation is guaranteed for the insertion part. The detection of the
   * pre-existing key and thus skipping of the insertion is atomic. However, in such a case a get will have to be
   * performed to retrieve the pre-existing value. This get is not atomic and could see the key as deleted, in which
   * case it will return a {@link ValueWrapper} with a null content.
   */
  @Override
  public ValueWrapper putIfAbsent(Object key, Object value) {
    if (value != null && !(value instanceof Serializable)) {
      throw new IllegalArgumentException(String.format("Value %s of type %s is not Serializable", value.toString(), value.getClass().getName()));
    }
    String documentId = getDocumentId(key.toString());
    SerializableDocument doc = SerializableDocument.create(documentId, ttl, (Serializable) value);

    try {
      client.insert(doc);
      return null;
    } catch (DocumentAlreadyExistsException e) {
      SerializableDocument existingDoc = client.get(documentId, SerializableDocument.class);
      if (existingDoc == null) {
        return new SimpleValueWrapper(null);
      }
      return new SimpleValueWrapper(existingDoc.content());
    }
  }

  /**
   * Construct the full Couchbase key for the given cache key. Given a {@link #DELIMITER} of ':', such keys
   * are in the form <code>CACHE_PREFIX:CACHE_NAME:key</code>. If the cache doesn't have a name it'll be
   * <code>CACHE_PREFIX::key</code>.
   *
   * @param key the cache key to transform to a Couchbase key.
   * @return the Couchbase key to use for storage.
   */
  private String getDocumentId(String key) {
    if(name == null || name.trim().length() == 0)
      return CACHE_PREFIX + DELIMITER + DELIMITER + key;
    else
      return CACHE_PREFIX + DELIMITER + name + DELIMITER + key;
  }

  private void evictAllDocuments() {
    ViewQuery query = ViewQuery.from(CACHE_DESIGN_DOCUMENT, CACHE_VIEW);
    query.stale(Stale.FALSE);
    if (name == null || name.trim().length() == 0) {
      query.key("");
    } else {
      query.key(name);
    }

    ViewResult response = client.query(query);
    for(ViewRow row : response) {
      client.remove(row.id());
    }
  }

  private void ensureViewExists() {
    BucketManager bucketManager = client.bucketManager();
    DesignDocument doc = null;

    try {
      doc = bucketManager.getDesignDocument(CACHE_DESIGN_DOCUMENT);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to retrieve design document " + CACHE_DESIGN_DOCUMENT, e);
      }
    }

    if(doc != null) {
      for(View view : doc.views()) {
        if(CACHE_VIEW.equals(view.name()))
          return;
      }
    }

    String function = "function (doc, meta) {var tokens = meta.id.split('" + DELIMITER + "'); if(tokens.length > 2 && " +
        "tokens[0] == '" + CACHE_PREFIX + "') emit(tokens[1]);}";
    View v = DefaultView.create(CACHE_VIEW, function);

    if (doc == null) {
      List<View> viewList = new ArrayList<View>(1);
      viewList.add(v);
      doc = DesignDocument.create(CACHE_DESIGN_DOCUMENT, viewList);
    } else {
      doc.views().add(v);
    }

    bucketManager.upsertDesignDocument(doc);
  }

  /**
   * Gets whether the cache should always use the flush() method to clear all documents.
   *
   * @return returns whether the cache should always use the flush() method to clear all documents.
   */
  public Boolean getAlwaysFlush() {
    return alwaysFlush;
  }

  /**
   * Sets whether the cache should always use the flush() method to clear all documents.
   *
   * @param alwaysFlush Whether the cache should always use the flush() method to clear all documents.
   */
  public void setAlwaysFlush(Boolean alwaysFlush) {
    this.alwaysFlush = alwaysFlush;
  }

}
