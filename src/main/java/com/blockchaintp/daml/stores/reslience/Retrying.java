package com.blockchaintp.daml.stores.reslience;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedFunction0;
import io.vavr.CheckedRunnable;
import io.vavr.control.Try;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Retrying<K, V> implements Store<K, V> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(Retrying.class);
  protected final Store<K, V> store;
  private final Retry getRetry;
  private final Retry putRetry;

  public Retrying(Config config, Store<K, V> store) {
    this.store = store;

    this.getRetry = Retry
      .of(String.format("%s#get", store.getClass().getCanonicalName()),
        RetryConfig
          .custom()
          .maxAttempts(config.maxRetries)
          .retryOnException(StoreReadException.class::isInstance)
          .build());

    this.putRetry = Retry
      .of(String.format("%s#put", store.getClass().getCanonicalName()),
        RetryConfig
          .custom()
          .maxAttempts(config.maxRetries)
          .retryOnException(StoreWriteException.class::isInstance)
          .build());

    getRetry.getEventPublisher().onRetry(r ->
      LOG.info("Retrying {} attempt {} due to {}",
        r::getName,
        r::getNumberOfRetryAttempts,
        r::getLastThrowable,
        () -> r.getLastThrowable().getMessage()
      )
    );

    getRetry.getEventPublisher().onError(r ->
      LOG.error("Retrying {} aborted after {} attempts due to {}",
          r::getName,
          r::getNumberOfRetryAttempts,
          r::getLastThrowable,
          () -> r.getLastThrowable().getMessage()
        )
    );

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

  <T> T decorateGet(CheckedFunction0<T> f) throws StoreReadException {
    try {
      return getRetry
        .executeCheckedSupplier(f);
    } catch (StoreReadException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreReadException(e);
    }
  }

  @Override
  public Optional<Value<V>> get(Key<K> key) throws StoreReadException {
    return decorateGet(() -> store.get(key));
  }

  @Override
  public Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys) throws StoreReadException {
    return decorateGet(() -> store.get(listOfKeys));
  }

  <T> void decoratePut(CheckedRunnable f) throws StoreWriteException {
    try {
      putRetry
        .executeCheckedSupplier(() -> {
          /// We only have a checked Supplier<>, so return a null
          f.run();
          return null;
        });
    } catch (StoreWriteException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreWriteException(e);
    }
  }

  @Override
  public void put(Key<K> key, Value<V> value) throws StoreWriteException {
    decoratePut(() -> store.put(key, value));
  }

  @Override
  public void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    decoratePut(() -> store.put(listOfPairs));
  }

  public static class Config {
    public int maxRetries = 3;
  }
}
