package com.blockchaintp.daml.stores.reslience;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.common.collect.Sets;

public class Caching<K, V> implements Store<K, V> {

  private final Store<K, V> store;
  private final LRUCache<K, V> innerCache;

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
