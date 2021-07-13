import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QldbRetryStrategy;
import com.blockchaintp.daml.stores.reslience.Retrying;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class QldbPutPagingTest {
  /**
   * A Stub store that only accepts put batches of 5 or fewer items
   */
  class CapacityLimitedStore implements Store<String, String> {
    private final Store<String, String> inner;

    CapacityLimitedStore() {
      inner = new StubStore<String,String>();
    }
    @Override
    public Optional<Value<String>> get(Key<String> key) throws StoreReadException {
      return inner.get(key);
    }

    @Override
    public Map<Key<String>, Value<String>> get(List<Key<String>> listOfKeys) throws StoreReadException {
      return inner.get(listOfKeys);
    }

    @Override
    public void put(Key<String> key, Value<String> value) throws StoreWriteException {
      inner.put(key,value);
    }

    @Override
    public void put(List<Map.Entry<Key<String>, Value<String>>> listOfPairs) throws StoreWriteException {
      if (listOfPairs.size() > 5) {
        throw new StoreWriteException(CapacityExceededException.builder().build());
      }

      inner.put(listOfPairs);
    }

    @Override
    public void sendEvent(String topic, String data) throws StoreWriteException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  void qldb_retry_subdivdes_pages_until_comitted() throws StoreWriteException, StoreReadException {
    var toCommit = new ArrayList<Map.Entry<Key<String>,Value<String>>>();
    for (int i = 0; i != 80; i++) {
      toCommit.add(new AbstractMap.SimpleEntry<>(
        Key.of(String.format("%d",i)),
        Value.of(String.format("%d",i))
      ));
    }
    var store = new CapacityLimitedStore();

    var qldbSubdividing = new QldbRetryStrategy(new Retrying.Config(),store);

    qldbSubdividing.put(toCommit);

    Assertions.assertEquals(
      80,
       store.get(toCommit.stream().map(kv -> kv.getKey())
         .collect(Collectors.toList())).values().size()
    );

  }


  @Test
  public void put_retries_configured_number_of_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new QldbRetryStrategy(
      new Retrying.Config(),
      store
    );

    doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doNothing()
      .when(store).put(any(List.class));

    /// List put
    Assertions.assertDoesNotThrow(
      () -> retrying.put(Arrays.asList())
    );
  }


  @Test
  public void put_eventually_fails_with_a_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new Retrying(
      new Retrying.Config(),
      store
    );

    doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doNothing()
      .when(store).put(any(List.class));

    /// List put
    var putMultipleEx = Assertions.assertThrows(
      StoreWriteException.class,
      () -> retrying.put(Arrays.asList())
    );

    Assertions.assertInstanceOf(
      S3Exception.class,
      putMultipleEx.getCause()
    );

  }
}
