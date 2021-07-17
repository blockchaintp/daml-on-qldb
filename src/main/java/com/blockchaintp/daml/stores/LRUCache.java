package com.blockchaintp.daml.stores;

import java.util.LinkedHashMap;
import java.util.Map;

import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;

public class LRUCache<K, V> extends LinkedHashMap<Key<K>, Value<V>> {
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final int INITIAL_SIZE = 16;
  private static final long serialVersionUID = 1L;
  private int cacheSize;

  public LRUCache(final int maxSize) {
    super(INITIAL_SIZE, DEFAULT_LOAD_FACTOR, true);
    this.cacheSize = maxSize;
  }

  @Override
  protected final boolean removeEldestEntry(final Map.Entry<Key<K>, Value<V>> eldest) {
    return size() >= cacheSize;
  }
}
