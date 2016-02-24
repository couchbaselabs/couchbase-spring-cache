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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.QueryExecutionException;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

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

  private static final ValueWrapper EMPTY_WRAPPER = new SimpleValueWrapper(null);
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

    if (doc.content() == null) {
      return EMPTY_WRAPPER;
    }
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

  //TODO in 4.3, override and throw a ValueRetrievalException
  /**
   * Return the value to which this cache maps the specified key, obtaining
   * that value from {@code valueLoader} if necessary. This method provides
   * a simple substitute for the conventional "if cached, return; otherwise
   * create, cache and return" pattern.
   * <p>If possible, implementations should ensure that the loading operation
   * is synchronized so that the specified {@code valueLoader} is only called
   * once in case of concurrent access on the same key.
   * <p>If the {@code valueLoader} throws an exception, it is wrapped in
   * a {@link RuntimeException}
   *
   * @param key the key whose associated value is to be returned
   * @param valueLoader
   * @return the value to which this cache maps the specified key
   * @throws RuntimeException if the {@code valueLoader} throws an exception
   * @since 4.3
   */
//  @Override
  public <T> T get(final Object key, final Callable<T> valueLoader) {
    final String documentId = getDocumentId(key.toString());
    SerializableDocument doc = client.get(documentId, SerializableDocument.class);
    if (doc == null && valueLoader != null) {
      synchronized (client) {
        doc = client.get(documentId, SerializableDocument.class);
        if (doc == null) {
          try {
            T value = valueLoader.call();
            put(key, value);
            return value;
          //TODO in 4.3, both RuntimeException below should be replaced by ValueRetrievalException
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
//            throw new ValueRetrievalException(key, valueLoader, e);
            throw new RuntimeException(String.format("Failed to load key %s using valueLoader %s", key, valueLoader.getClass().getName()));
          }
        }
      }
    }

    if (doc != null) {
      return (T) doc.content();
    }
    return null;
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
   * <p>
   * If {@link #setAlwaysFlush(Boolean)} is set to true, the underlying {@link Bucket} is flushed, which is a very
   * destructive action, so only use it with care (for instance other caches or clients may store unrelated data in the
   * same Bucket as this cache, and flushing will also destroy said data). Note that flush may not be enabled on the
   * underlying bucket.
   * <p>
   * Otherwise, a Couchbase {@link View} is used to choose which documents to remove from the underlying storage.
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
        return EMPTY_WRAPPER;
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
  protected String getDocumentId(String key) {
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

    client.async()
        .query(query)
        .flatMap(ROW_IDS_OR_ERROR)
        .flatMap(new Func1<String, Observable<? extends Document>>() {
          @Override
          public Observable<? extends Document> call(String id) {
            return client.async().remove(id, SerializableDocument.class);
          }
        })
        .toBlocking()
        .lastOrDefault(null); //ignore empty cache
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
   * <p>
   * This is a very destructive action, so only use it with care (for instance other caches or clients may store
   * unrelated data in the same Bucket as this cache, and flushing will also destroy said data). Note that flush may not
   * be enabled on the underlying bucket.
   *
   * @param alwaysFlush Whether the cache should always use the flush() method to clear all documents.
   */
  public void setAlwaysFlush(Boolean alwaysFlush) {
    this.alwaysFlush = alwaysFlush;
  }


  /** Converts View Rows to the associated document's ID */
  private static final Func1<AsyncViewRow, String> ROW_TO_ID =
      new Func1<AsyncViewRow, String>() {
        @Override
        public String call(AsyncViewRow asyncViewRow) {
          return asyncViewRow.id();
        }
      };

  /**
   * Converts a JsonObject view error into an Observable&lt;String&gt; that emits
   * a{@link com.couchbase.client.java.error.QueryExecutionException} wrapping the error.
   */
  private static final Func1<JsonObject, Observable<String>> JSON_TO_ONERROR =
      new Func1<JsonObject, Observable<String>>() {
        @Override
        public Observable<String> call(JsonObject jsonError) {
          return Observable.error(new QueryExecutionException(
              "Error during view query execution: ", jsonError));
        }
      };

  /**
   * Out of an {@link AsyncViewResult}, extract the stream of document IDs or emit an error if unsuccessful.
   */
  private static Func1<AsyncViewResult, Observable<String>> ROW_IDS_OR_ERROR =
      new Func1<AsyncViewResult, Observable<String>>() {
          @Override
          public Observable<String> call(AsyncViewResult asyncViewResult) {
            if (asyncViewResult.success()) {
              return asyncViewResult
                  .rows()
                  .map(ROW_TO_ID);
            } else {
              return asyncViewResult.error()
                  .flatMap(JSON_TO_ONERROR);
            }
          }
        };

}
