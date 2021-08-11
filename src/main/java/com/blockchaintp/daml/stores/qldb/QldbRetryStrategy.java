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
package com.blockchaintp.daml.stores.qldb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.RetryingConfig;
import com.blockchaintp.daml.stores.layers.RetryingStore;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;

/**
 * QLDB has some interesting quota behavior that demands specific retry. strategies
 *
 * @param <K>
 *          type of Key
 * @param <V>
 *          type of Value
 * @see <a href= "https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html">QLDB
 *      Driver errors</a>
 */
public class QldbRetryStrategy<K, V> extends RetryingStore<K, V> implements Store<K, V> {
  private static final int DEFAULT_MAX_DOCUMENTS = 40;
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbRetryStrategy.class);

  /**
   * Constructor.
   *
   * @param config
   *          the retry config
   * @param store
   *          the store, specifically meant to be used with a QLDBStore, but not required to do so
   */
  public QldbRetryStrategy(final RetryingConfig config, final Store<K, V> store) {
    super(config, store);

    Retry putRetry = Retry.of(String.format("%s#put-qldb-batch", store.getClass().getCanonicalName()),
        RetryConfig.custom().maxAttempts(config.getMaxRetries())
            .retryOnException(QldbRetryStrategy::specificallyHandleCapacityExceptions).build());

    putRetry.getEventPublisher().onRetry(r -> LOG.info("Retrying {} attempt {} due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> Objects.requireNonNull(r.getLastThrowable()).getMessage()));

    putRetry.getEventPublisher().onError(r -> LOG.error("Retrying {} aborted after {} attempts due to {}", r::getName,
        r::getNumberOfRetryAttempts, () -> Objects.requireNonNull(r.getLastThrowable()).getMessage()));
  }

  private static boolean specificallyHandleCapacityExceptions(final Throwable e) {
    return e instanceof StoreWriteException && !(e.getCause() instanceof CapacityExceededException);
  }

  final List<List<Map.Entry<Key<K>, Value<V>>>> pageBy(final int size,
      final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) {
    int page = 0;
    var pages = new ArrayList<List<Map.Entry<Key<K>, Value<V>>>>();
    while (true) {
      var nextPage = listOfPairs.stream().skip((long) page * size).limit(size).collect(Collectors.toList());

      if (nextPage.isEmpty()) {
        break;
      }

      pages.add(page, nextPage);
      page += 1;
    }

    return pages;
  }

  final void putImpl(final int pageSize, final List<Map.Entry<Key<K>, Value<V>>> listOfPairs)
      throws StoreWriteException {
    var pages = pageBy(pageSize, listOfPairs);

    if (pages.size() > 1) {
      LOG.info("Paging commit of {} x {}", pages::size, () -> pageSize);
    }

    for (var page : pages) {
      try {
        getStore().put(page);
      } catch (StoreWriteException e) {
        if (e.getCause() instanceof CapacityExceededException) {
          /// Subdivide this page if possible, otherwise abort
          if (pageSize > 1) {
            LOG.info("Capacity exception at page size {}, subdividing into two pages and retrying", () -> pageSize);
            putImpl(pageSize / 2, page);
          }
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * QLDB has unpredictable transactional quotas, depending on delta binary size that we cannot
   * determine up front Split any batch into batches of 40 items (the Qldb max), if these fail with
   * CapacityExceededException then subdivide further. Other exceptional conditions can be retried in
   * standard ways
   */
  @Override
  public void put(final List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    putImpl(DEFAULT_MAX_DOCUMENTS, listOfPairs);
  }
}
