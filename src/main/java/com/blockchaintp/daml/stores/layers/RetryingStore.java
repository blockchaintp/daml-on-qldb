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
import io.vavr.CheckedFunction0;
import io.vavr.CheckedRunnable;
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

    this.putRetry = Retry.of(String.format("%s#put", store.getClass().getCanonicalName()), RetryConfig.custom()
        .maxAttempts(config.getMaxRetries()).retryOnException(StoreWriteException.class::isInstance).build());

    getRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable, () -> r.getLastThrowable().getMessage()));

    getRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, r::getLastThrowable, () -> r.getLastThrowable().getMessage()));

    putRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> r.getLastThrowable().getMessage()));

    putRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> r.getLastThrowable().getMessage()));
  }

  /**
   * @return the underlying store
   */
  protected Store<K, V> getStore() {
    return store;
  }

  final <T> T decorateGet(final CheckedFunction0<T> f) throws StoreReadException {
    try {
      return getRetry.executeSupplier(f.unchecked());
    } catch (RuntimeException e) {
      throw new StoreReadException(e);
    } catch (Exception e) {
      if (e instanceof StoreReadException) {
        throw e;
      }
      throw new StoreReadException(e);
    }
  }

  @Override
  public final Optional<Value<V>> get(final Key<K> key) throws StoreReadException {
    return decorateGet(() -> store.get(key));
  }

  @Override
  public final Map<Key<K>, Value<V>> get(final List<Key<K>> listOfKeys) throws StoreReadException {
    return decorateGet(() -> store.get(listOfKeys));
  }

  final void decoratePut(final CheckedRunnable f) throws StoreWriteException {
    try {
      putRetry.executeSupplier(() -> {
        /// We only have a checked Supplier<>, so return a null
        f.unchecked().run();
        return null;
      });
    } catch (RuntimeException e) {
      throw new StoreWriteException(e);
    } catch (Exception e) {
      if (e instanceof StoreReadException) {
        throw e;
      }
      throw new StoreWriteException(e);
    }
  }

  @Override
  public final void put(final Key<K> key, final Value<V> value) throws StoreWriteException {
    decoratePut(() -> store.put(key, value));
  }

  /**
   * This may be overriden by subclasses to provide a different put implementation.
   */
  @Override
  public void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    decoratePut(() -> store.put(listOfPairs));
  }

}
