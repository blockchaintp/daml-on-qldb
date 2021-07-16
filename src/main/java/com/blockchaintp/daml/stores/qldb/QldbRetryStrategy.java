package com.blockchaintp.daml.stores.qldb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.reslience.Retrying;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;

/**
 * QLDB has some interesting quota behaviour that demands specific retry strategies
 *
 * @see <a href="https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html">QLDB Driver errors</a>
 */
public class QldbRetryStrategy<K,V> extends Retrying<K,V> implements TransactionLog<K,V> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(QldbRetryStrategy.class);
  private final Retry putRetry;
  int qldbMaxDocuments = 40;

  public QldbRetryStrategy(Config config, Store<K, V> store) {
    super(config, store);

    this.putRetry = Retry.of(String.format("%s#put-qldb-batch", store.getClass().getCanonicalName()),
      RetryConfig
        .custom()
        .maxAttempts(config.maxRetries)
        .retryOnException(QldbRetryStrategy::specificallyHandleCapacityExceptions)
        .build());

    putRetry.getEventPublisher().onRetry(r ->
      LOG.info("Retrying {} attempt {} due to {}",
        r::getName,
        r::getNumberOfRetryAttempts,
        () -> r.getLastThrowable().getMessage()
      )
    );

    putRetry.getEventPublisher().onError(r ->
      LOG.error("Retrying {} aborted after {} attempts due to {}",
        r::getName,
        r::getNumberOfRetryAttempts,
        () -> r.getLastThrowable().getMessage()
      )
    );
  }

  private static boolean specificallyHandleCapacityExceptions(Throwable e) {
    return e instanceof StoreWriteException && !(e.getCause() instanceof CapacityExceededException);
  }


  List<List<Map.Entry<Key<K>, Value<V>>>> pageBy(
    int size,
    List<Map.Entry<Key<K>, Value<V>>> listOfPairs
  ) {
    int page = 0;
    var pages = new ArrayList<List<Map.Entry<Key<K>, Value<V>>>>();
    while (true) {
      var nextPage = listOfPairs
        .stream()
        .skip((long) page * size)
        .limit(size)
        .collect(Collectors.toList());

      if (nextPage.isEmpty()) {
        break;
      }

      pages.add(page, nextPage);
      page += 1;
    }

    return pages;
  }


  void putImpl(int pageSize, List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    var pages = pageBy(pageSize, listOfPairs);

    if (pages.size() > 1) {
      LOG.info("Paging commit of {} x {}", ()-> pages.size(),() -> pageSize);
    }

    for (var page : pages) {
      try {
        store.put(page);
        //Retry
       //   .decorateCheckedRunnable(putRetry, () -> store.put(page))
       //   .run();
      } catch (Throwable e) {
        if (e.getCause() instanceof CapacityExceededException) {
          /// Subdivide this page if possible, otherwise abort
          if (pageSize > 1) {
            LOG.info("Capacity exception at page size {}, subdividing into two pages and retyring", () -> pageSize);
            putImpl(pageSize / 2, page);
          }
        } else {
          /// Avoid double wrapping and rethrow
          if (e instanceof StoreWriteException) {
            throw (StoreWriteException) e;
          }

          throw new StoreWriteException(e);
        }
      }
    }
  }

  /**
   * QLDB has unpredictable transactional quotas, depending on delta binary size that we cannot determine up front
   * Split any batch into batches of 40 items (the Qldb max), if these fail with CapacityExceededException then subdivide further. Other
   * exceptional conditions can be retried in standard ways
   */
  @Override
  public void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    putImpl(this.qldbMaxDocuments, listOfPairs);
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {

  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {

  }
}
