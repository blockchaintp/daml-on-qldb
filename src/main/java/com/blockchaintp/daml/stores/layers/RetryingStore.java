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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * A {@link Store} layer which retries the read operation if an exception occurs.
 *
 * @param <K>
 *          Key type
 * @param <V>
 *          Value type
 */
public class RetryingStore<K, V> implements Store<K, V> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(RetryingStore.class);
  private final Store<K, V> store;

  private final Retry getRetry;
  private final Retry putRetry;

  /**
   * Construct the {@link RetryingStore} layer around the provided {@link Store}.
   *
   * @param config
   *          the configuration for the retry
   * @param wrappedStore
   *          the {@link Store} to wrap
   */
  public RetryingStore(final RetryingConfig config, final Store<K, V> wrappedStore) {
    this.store = wrappedStore;

    this.getRetry = Retry.of(String.format("%s#get", store.getClass().getCanonicalName()), RetryConfig.custom()
        .maxAttempts(config.getMaxRetries()).retryOnException(StoreReadException.class::isInstance).build());

    this.putRetry = Retry.of(String.format("%s#put", store.getClass().getCanonicalName()),
        RetryConfig.custom().maxAttempts(config.getMaxRetries()).retryOnException(s -> true).build());

    getRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable));

    getRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable));

    putRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable));

    putRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable));
  }

  /**
   * @return the underlying store
   */
  protected Store<K, V> getStore() {
    return store;
  }

  @Override
  public final Optional<Value<V>> get(final Key<K> key) throws StoreReadException {
    return WrapFunction0
        .of(Retry.decorateCheckedSupplier(getRetry, () -> store.get(key)).unchecked(), StoreReadException::new).apply();
  }

  @Override
  public final Map<Key<K>, Value<V>> get(final List<Key<K>> listOfKeys) throws StoreReadException {
    return WrapFunction0
        .of(Retry.decorateCheckedSupplier(getRetry, () -> store.get(listOfKeys)).unchecked(), StoreReadException::new)
        .apply();
  }

  @Override
  public final void put(final Key<K> key, final Value<V> value) throws StoreWriteException {
    WrapRunnable
        .of(Retry.decorateCheckedRunnable(putRetry, () -> store.put(key, value)).unchecked(), StoreWriteException::new)
        .run();
  }

  /**
   * It is useful to override here for paging behaviour.
   *
   * @param listOfPairs
   *          the list of K/V pairs to put
   * @throws StoreWriteException
   */
  @Override
  public void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    WrapRunnable
        .of(Retry.decorateCheckedRunnable(putRetry, () -> store.put(listOfPairs)).unchecked(), StoreWriteException::new)
        .run();
  }

}
