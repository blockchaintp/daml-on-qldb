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

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;

import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;

/**
 *
 * @param <K>
 * @param <V>
 * @param <I>
 */
public final class CachingTransactionLog<K, V, I> implements TransactionLog<K, V, I> {
  private final LRUCache<I, Tuple2<K, V>> commitCache;
  private final LRUCache<K, V> eventCache;
  private final TransactionLog<K, V, I> inner;
  private final Function2<I, Optional<I>, Stream<I>> genRange;

  /**
   * Build a caching transaction log from a cache of events and commits. Our goal is to be able to
   * immediately return recently committed transactions to subscribers.
   *
   * @param theCommitCache
   * @param theEventCache
   * @param theInner
   * @param theGenRange
   */
  public CachingTransactionLog(final LRUCache<I, Tuple2<K, V>> theCommitCache, final LRUCache<K, V> theEventCache,
      final TransactionLog<K, V, I> theInner, final Function2<I, Optional<I>, Stream<I>> theGenRange) {
    this.commitCache = theCommitCache;
    eventCache = theEventCache;
    inner = theInner;
    genRange = theGenRange;
  }

  /**
   * Attempt to retrieve range from the commit cache, if any miss then read through the whole batch.
   *
   * @param startExclusive
   * @param endInclusive
   * @return A stream of potentially cached items.
   * @throws StoreReadException
   */
  @Override
  public Stream<Tuple3<I, K, V>> from(final I startExclusive, final Optional<I> endInclusive)
      throws StoreReadException {

    var hit = genRange.apply(startExclusive, endInclusive).map(i -> Tuple.of(i, commitCache.get(Key.of(i))))
        .collect(Collectors.toList());

    if (hit.stream().anyMatch(i -> i._2 == null)) {
      return inner.from(startExclusive, endInclusive);
    }

    return hit.stream().map(i -> Tuple.of(i._1, i._2.toNative()._1, i._2.toNative()._2)).map(v -> {
      commitCache.put(Key.of(v._1), Value.of(Tuple.of(v._2, v._3)));
      return v;
    });
  }

  @Override
  public Optional<I> getLatestOffset() {
    return inner.getLatestOffset();
  }

  @Override
  public Tuple2<K, I> begin(final Optional<K> id) throws StoreWriteException {
    return inner.begin(id);
  }

  @Override
  public void sendEvent(final K id, final V data) throws StoreWriteException {
    inner.sendEvent(id, data);
    eventCache.put(Key.of(id), Value.of(data));
  }

  @Override
  public I commit(final K txId) throws StoreWriteException {
    var offset = inner.commit(txId);
    var last = eventCache.get(Key.of(txId));

    if (last != null) {
      eventCache.remove(Key.of(txId));
      commitCache.put(Key.of(offset), Value.of(Tuple.of(txId, last.toNative())));
    }

    return offset;
  }

  @Override
  public void abort(final K txId) throws StoreWriteException {
    inner.abort(txId);
  }
}
