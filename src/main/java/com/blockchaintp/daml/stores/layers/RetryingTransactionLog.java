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

import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.rxjava3.retry.transformer.RetryTransformer;
import io.reactivex.rxjava3.core.Observable;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * Retrying semantics for transaction log writes and its observable stream.
 *
 * @param <K>
 * @param <V>
 * @param <I>
 */
public final class RetryingTransactionLog<K, V, I> implements TransactionLog<K, V, I> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(RetryingTransactionLog.class);
  private final TransactionLog<K, V, I> store;
  private final RetryingConfig config;

  private Retry retry(final String name) {
    var writeRetry = Retry.of(String.format("%s#%s", store.getClass().getCanonicalName(), name), RetryConfig.custom()
        .maxAttempts(config.getMaxRetries()).retryOnException(StoreWriteException.class::isInstance).build());

    writeRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> r.getLastThrowable().getMessage()));

    writeRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> r.getLastThrowable().getMessage()));

    return writeRetry;
  }

  /**
   * Construct a new retrying wrapper from configuration and an underlying store.
   *
   * @param theConfig
   * @param theStore
   */
  public RetryingTransactionLog(final RetryingConfig theConfig, final TransactionLog<K, V, I> theStore) {
    this.store = theStore;
    this.config = theConfig;
  }

  @Override
  public Observable<Map.Entry<K, V>> from(final Optional<I> offset) {
    RetryTransformer<Map.Entry<K, V>> retrying = RetryTransformer.of(retry("from"));

    return Observable.wrap(retrying.apply(store.from(offset)));
  }

  @Override
  public K begin() throws StoreWriteException {
    return Retry.decorateCheckedSupplier(retry("begin"), store::begin).unchecked().apply();
  }

  @Override
  public void sendEvent(final K id, final V data) throws StoreWriteException {
    Retry.decorateCheckedRunnable(retry("sendEvent"), () -> store.sendEvent(id, data)).unchecked().run();
  }

  @Override
  public I commit(final K txId) throws StoreWriteException {
    return Retry.decorateCheckedSupplier(retry("commit"), () -> store.commit(txId)).unchecked().apply();
  }

  @Override
  public void abort(final K txId) throws StoreWriteException {
    Retry.decorateCheckedRunnable(retry("abort"), () -> store.abort(txId)).unchecked().run();
  }
}
