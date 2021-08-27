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
package com.blockchaintp.daml.participant;

import java.util.Optional;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;

import scala.Function1;
import scala.Option;
import scala.jdk.javaapi.OptionConverters$;

/**
 * Simple locking state cache.
 *
 * @param <K>
 * @param <V>
 */
public final class StateCache<K, V> extends com.daml.caching.ConcurrentCache<K, V> {
  private LRUCache<K, V> cache;

  /**
   * Wrap an LRU cache with a daml compatible ConcurrentCache.
   *
   * @param theCache
   */
  public StateCache(final LRUCache<K, V> theCache) {
    cache = theCache;
  }

  @Override
  public void put(final K theO, final V theO2) {
    synchronized (cache) {
      cache.put(Key.of(theO), Value.of(theO2));
    }
  }

  @Override
  public Option<V> getIfPresent(final K theO) {
    synchronized (cache) {
      return OptionConverters$.MODULE$.toScala(Optional.ofNullable(cache.get(theO)).map(x -> x.toNative()));
    }
  }

  @Override
  public V getOrAcquire(final K theK, final Function1<K, V> acquire) {
    synchronized (cache) {
      var hit = cache.get(theK);
      if (hit != null) {
        return hit.toNative();
      }
      var value = Value.of(acquire.apply(theK));
      cache.put(Key.of(theK), value);

      return value.toNative();
    }
  }
}
