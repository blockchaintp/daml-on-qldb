package com.blockchaintp.daml.stores.reslience;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedFunction0;
import io.vavr.CheckedRunnable;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

public class Retrying<K, V> implements Store<K, V> {

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(Retrying.class);
  private final Store<K, V> store;

  private final Retry getRetry;
  private final Retry putRetry;

  public Retrying(final Config config, final Store<K, V> wrappedStore) {
    this.store = wrappedStore;

    this.getRetry = Retry.of(String.format("%s#get", store.getClass().getCanonicalName()), RetryConfig.custom()
        .maxAttempts(config.maxRetries).retryOnException(StoreReadException.class::isInstance).build());

    this.putRetry = Retry.of(String.format("%s#put", store.getClass().getCanonicalName()), RetryConfig.custom()
        .maxAttempts(config.maxRetries).retryOnException(StoreWriteException.class::isInstance).build());

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
      return getRetry.executeCheckedSupplier(f);
    } catch (StoreReadException e) {
      throw e;
    } catch (Throwable e) {
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
      putRetry.executeCheckedSupplier(() -> {
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

  public static class Config {
    private static final int DEFAULT_MAX_RETRIES = 3;
    /**
     * The maximum number of retries.
     */
    public int maxRetries = DEFAULT_MAX_RETRIES;
  }
}
