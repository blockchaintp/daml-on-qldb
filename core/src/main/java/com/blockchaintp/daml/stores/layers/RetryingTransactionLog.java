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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.blockchaintp.daml.participant.SubmissionStatus;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.Tuple3;
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
  private static final long RETRY_DURATION = 2000L;
  private static final int RETRY_JITTER = 100;
  private long retryDuration = RETRY_DURATION;
  private int retryJitter = RETRY_JITTER;
  private final TransactionLog<K, V, I> store;
  private final RetryConfig config;

  private Retry retry(final String name) {
    var writeRetry = Retry.of(String.format("%s#%s", store.getClass().getCanonicalName(), name), config);

    writeRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> Objects.requireNonNull(r.getLastThrowable()).getMessage()));

    writeRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> Objects.requireNonNull(r.getLastThrowable()).getMessage()));

    return writeRetry;
  }

  /**
   * Construct a new retrying wrapper from configuration and an underlying store.
   *
   * @param theRetries
   * @param theStore
   */
  public RetryingTransactionLog(final int theRetries, final TransactionLog<K, V, I> theStore) {
    store = theStore;
    config = RetryConfig.custom().maxAttempts(theRetries).retryOnResult(x -> x == SubmissionStatus.OVERLOADED)
        .writableStackTraceEnabled(false)
        .waitDuration(Duration.ofMillis((retryDuration + (long) (Math.random() % retryJitter))))
        .retryOnException(e -> true).build();
  }

  @Override
  public Stream<Tuple3<I, K, V>> from(final I startExclusive,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<I> endInclusive)
      throws StoreReadException {
    return store.from(startExclusive, endInclusive);
  }

  @Override
  public Optional<I> getLatestOffset() {
    return store.getLatestOffset();
  }

  @Override
  public K begin(final Optional<K> id) throws StoreWriteException {
    return WrapFunction0
        .of(Retry.decorateCheckedSupplier(retry("begin"), () -> store.begin(id)).unchecked(), StoreWriteException::new)
        .apply();
  }

  @Override
  public void sendEvent(final K id, final V data) throws StoreWriteException {
    WrapRunnable.of(Retry.decorateCheckedRunnable(retry("sendEvent"), () -> store.sendEvent(id, data)).unchecked(),
        StoreWriteException::new).run();
  }

  @Override
  public I commit(final K txId) throws StoreWriteException {
    return WrapFunction0.of(Retry.decorateCheckedSupplier(retry("commit"), () -> store.commit(txId)).unchecked(),
        StoreWriteException::new).apply();
  }

  @Override
  public void abort(final K txId) throws StoreWriteException {
    WrapRunnable.of(Retry.decorateCheckedRunnable(retry("abort"), () -> store.abort(txId)).unchecked(),
        StoreWriteException::new).run();
  }
}
