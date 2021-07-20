/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.stores.layers;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Layer that caches the results of a store.
 *
 * @param <K>
 *          the type of key
 * @param <V>
 *          the type of value
 */
public class Caching<K, V> implements Store<K, V> {

  private final Store<K, V> store;
  private final LRUCache<K, V> innerCache;

  /**
   * Build a cache for the given store using the given cache.
   *
   * @param cache
   *          the cache to use
   * @param wrappedStore
   *          the store to cache
   */
  public Caching(final LRUCache<K, V> cache, final Store<K, V> wrappedStore) {
    this.store = wrappedStore;
    innerCache = cache;
  }

  /**
   * @return the store
   */
  protected Store<K, V> getStore() {
    return store;
  }

  @Override
  public final Optional<Value<V>> get(final Key<K> key) throws StoreReadException {
    var hit = false;
    synchronized (innerCache) {
      hit = innerCache.containsKey(key);
    }
    if (hit) {
      synchronized (innerCache) {
        return Optional.of(innerCache.get(key));
      }
    } else {
      var val = store.get(key);
      synchronized (innerCache) {
        val.ifPresent(vValue -> innerCache.put(key, vValue));

        return val;
      }
    }
  }

  @Override
  public final Map<Key<K>, Value<V>> get(final List<Key<K>> listOfKeys) throws StoreReadException {
    try {
      var all = new HashSet<>(listOfKeys);
      var map = new HashMap<Key<K>, Value<V>>();
      var hits = new HashMap<Key<K>, Value<V>>();

      synchronized (innerCache) {
        all.stream().filter(innerCache::containsKey).forEach(k -> hits.put(k, innerCache.get(k)));
      }
      var misses = Sets.difference(all, hits.keySet());
      var readThrough = store.get(new ArrayList<>(misses));

      synchronized (innerCache) {
        innerCache.putAll(readThrough);
      }

      hits.forEach(map::put);
      readThrough.forEach(map::put);

      return map;

    } catch (RuntimeException e) {
      throw new StoreReadException("Exception while reading from store", e);
    }
  }

  @Override
  public final void put(final Key<K> key, final Value<V> value) throws StoreWriteException {
    store.put(key, value);

    synchronized (innerCache) {
      innerCache.put(key, value);
    }
  }

  @Override
  public final void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    store.put(listOfPairs);

    synchronized (innerCache) {
      listOfPairs.forEach(kv -> innerCache.put(kv.getKey(), kv.getValue()));
    }
  }

}
