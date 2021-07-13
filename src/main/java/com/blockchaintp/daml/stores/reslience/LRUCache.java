package com.blockchaintp.daml.stores.reslience;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<Key<K>, Value<V>> {
  private static final long serialVersionUID = 1L;
  private int cacheSize;

  public LRUCache(int cacheSize) {
    super(16, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  protected boolean removeEldestEntry(Map.Entry<Key<K>, Value<V>> eldest) {
    return size() >= cacheSize;
  }
}
