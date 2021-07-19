package com.blockchaintp.daml.stores.resilience;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.Retrying;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class RetryingTest {
  @Test
  void get_retries_configured_number_of_store_read_exceptions() throws StoreReadException {
    var store = mock(Store.class);
    var retrying = new Retrying(
      new Retrying.Config(),
      store
    );

    when(store.get(any(Key.class)))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenReturn(Optional.of(new Value<>("stuff")));

    var retMap = new HashMap<>();
    retMap.put(new Key<>("stuff"), new Value<>("stuff"));
    when(store.get(any(List.class)))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenReturn(retMap);

    /// Scalar get
    Assertions.assertEquals(
      Optional.of(new Value<>("stuff")),
      retrying.get(new Key<>(""))
    );

    /// List get
    Assertions.assertEquals(
      retMap,
      retrying.get(Arrays.asList(new Key<>("stuff")))
    );
  }

  @Test
  void get_eventually_fails_with_a_store_read_exception() throws StoreReadException {
    var store = mock(Store.class);
    var retrying = new Retrying(
      new Retrying.Config(),
      store
    );

    when(store.get(any(Key.class)))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenReturn(Optional.of(new Value<>("stuff")));


    when(store.get(any(List.class)))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenThrow(new StoreReadException(S3Exception.builder().build()))
      .thenReturn(new HashMap<>());

    var getEx = Assertions.assertThrows(
      StoreReadException.class,
      () -> retrying.get(new Key<>(""))
    );

    var getMultipleEx = Assertions.assertThrows(
      StoreReadException.class,
      () -> retrying.get(Arrays.asList())
    );

    /// Check we did not double wrap
    Assertions.assertInstanceOf(
      S3Exception.class,
      getEx.getCause()
    );

    Assertions.assertInstanceOf(
      S3Exception.class,
      getMultipleEx.getCause()
    );
  }

  @Test
  void put_retries_configured_number_of_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new Retrying(
      new Retrying.Config(),
      store
    );

    doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doNothing()
      .when(store).put(any(Key.class), any(Value.class));


    doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doNothing()
      .when(store).put(any(List.class));

    /// Scalar put
    Assertions.assertDoesNotThrow(
      () -> retrying.put(new Key<>(""), new Value<>(""))
    );

    /// List put
    Assertions.assertDoesNotThrow(
      () -> retrying.put(Arrays.asList())
    );
  }


  @Test
  void put_eventually_fails_with_a_store_write_exceptions() throws StoreWriteException {
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
      .when(store).put(any(Key.class), any(Value.class));


    doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doThrow(new StoreWriteException(S3Exception.builder().build()))
      .doNothing()
      .when(store).put(any(List.class));

    /// Scalar put
    var putEx = Assertions.assertThrows(
      StoreWriteException.class,
      () -> retrying.put(new Key<>(""), new Value<>(""))
    );

    /// List put
    var putMultipleEx = Assertions.assertThrows(
      StoreWriteException.class,
      () -> retrying.put(Arrays.asList())
    );

    /// Check we did not double wrap
    Assertions.assertInstanceOf(
      S3Exception.class,
      putEx.getCause()
    );

    Assertions.assertInstanceOf(
      S3Exception.class,
      putMultipleEx.getCause()
    );

  }
}
