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
package com.blockchaintp.daml.stores;

import java.util.LinkedHashMap;
import java.util.Map;

import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;

/**
 * A simple LRU cache.
 *
 * @param <K>
 *          the key type
 * @param <V>
 *          the value type
 */
public class LRUCache<K, V> extends LinkedHashMap<Key<K>, Value<V>> {
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final int INITIAL_SIZE = 16;
  private static final long serialVersionUID = 1L;
  private final int cacheSize;

  /**
   * Construct the cache with the specified max size.
   *
   * @param maxSize
   *          the maximum size of the cache
   */
  public LRUCache(final int maxSize) {
    super(INITIAL_SIZE, DEFAULT_LOAD_FACTOR, true);
    this.cacheSize = maxSize;
  }

  @Override
  protected final boolean removeEldestEntry(final Map.Entry<Key<K>, Value<V>> eldest) {
    return size() >= cacheSize;
  }

  @SuppressWarnings("EmptyMethod")
  @Override
  public final int hashCode() {
    return super.hashCode();
  }

  @Override
  public final boolean equals(final Object o) {
    // Two LRUCaches may be equal iff they are the same instance.
    return this == o;
  }
}
