package com.blockchaintp.daml.stores.qldb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.StubStore;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.Retrying;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@SuppressWarnings({ "unchecked", "rawtypes" })
class QldbPutPagingTest {
  /**
   * A Stub store that only accepts put batches of 5 or fewer items.
   */
  class CapacityLimitedStore implements TransactionLog<String, String> {
    private static final int MAX_CAPACITY = 5;
    private final Store<String, String> inner;

    CapacityLimitedStore() {
      inner = new StubStore<String, String>();
    }

    @Override
    public Optional<Value<String>> get(final Key<String> key) throws StoreReadException {
      return inner.get(key);
    }

    @Override
    public Map<Key<String>, Value<String>> get(final List<Key<String>> listOfKeys) throws StoreReadException {
      return inner.get(listOfKeys);
    }

    @Override
    public void put(final Key<String> key, final Value<String> value) throws StoreWriteException {
      inner.put(key, value);
    }

    @Override
    public void put(final List<Map.Entry<Key<String>, Value<String>>> listOfPairs) throws StoreWriteException {
      if (listOfPairs.size() > MAX_CAPACITY) {
        throw new StoreWriteException(CapacityExceededException.builder().build());
      }

      inner.put(listOfPairs);
    }

    @Override
    public void sendEvent(final String topic, final String data) throws StoreWriteException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendEvent(final List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
      throw new UnsupportedOperationException();
    }
  }

  private static final int ITERATIONS = 80;

  @Test
  void qldb_retry_subdivdes_pages_until_committed() throws StoreWriteException, StoreReadException {
    var toCommit = new ArrayList<Map.Entry<Key<String>, Value<String>>>();
    for (int i = 0; i < ITERATIONS; i++) {
      toCommit.add(new AbstractMap.SimpleEntry<>(Key.of(String.format("%d", i)), Value.of(String.format("%d", i))));
    }
    var store = new CapacityLimitedStore();

    var qldbSubdividing = new QldbRetryStrategy(new Retrying.Config(), store);

    qldbSubdividing.put(toCommit);

    Assertions.assertEquals(ITERATIONS,
        store.get(toCommit.stream().map(kv -> kv.getKey()).collect(Collectors.toList())).values().size());

  }

  @Test
  void put_retries_configured_number_of_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new QldbRetryStrategy(new Retrying.Config(), store);

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store).put(any(List.class));

    /// List put
    Assertions.assertDoesNotThrow(() -> retrying.put(Arrays.asList()));
  }

  @Test
  void put_eventually_fails_with_a_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new Retrying(new Retrying.Config(), store);

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store).put(any(List.class));

    /// List put
    var putMultipleEx = Assertions.assertThrows(StoreWriteException.class, () -> retrying.put(Arrays.asList()));

    Assertions.assertInstanceOf(S3Exception.class, putMultipleEx.getCause());

  }
}
