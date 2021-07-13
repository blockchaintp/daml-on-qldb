package com.blockchaintp.daml.stores.reslience;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.common.collect.Sets;

import java.util.*;

public class Caching<K, V> implements Store<K, V> {

  final Store<K, V> store;
  private final LRUCache<K, V> innerCache;

  public Caching(LRUCache<K,V> cache, Store<K, V> store) {
    this.store = store;
    innerCache = cache;
  }

  @Override
  public Optional<Value<V>> get(Key<K> key) throws StoreReadException {
    var hit = false;
    synchronized (innerCache) {
      hit = innerCache.containsKey(key);
    }
    if (hit) {
      synchronized (innerCache) {
        return Optional.of((Value<V>) innerCache.get(key));
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
  public Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys) throws StoreReadException {
    try {
      var all = new HashSet<>(listOfKeys);
      var map = new HashMap<Key<K>, Value<V>>();
      var hits = new HashMap<Key<K>,Value<V>>();

      synchronized (innerCache) {
          all
            .stream()
            .filter(innerCache::containsKey)
            .forEach(k -> hits.put(k,innerCache.get(k)));
      };

      var misses = Sets.difference(all, hits.keySet());
      var readThrough = store.get(
          new ArrayList<>(misses)
      );

      synchronized (innerCache) {
        innerCache.putAll(readThrough);
      }

      hits.forEach((k, v) -> map.put((Key<K>) k, (Value<V>) v));
      readThrough.forEach(map::put);

      return map;

    } catch (StoreReadException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreReadException(e);
    }
  }

  @Override
  public void put(Key<K> key, Value<V> value) throws StoreWriteException {
    store.put(key, value);

    synchronized (innerCache) {
      innerCache.put(key, value);
    }
  }

  @Override
  public void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    store.put(listOfPairs);

    synchronized (innerCache) {
      listOfPairs.forEach(kv -> innerCache.put(kv.getKey(), kv.getValue()));
    }
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }
}
