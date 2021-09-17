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
package com.blockchaintp.daml.stores.resilience;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.RetryingConfig;
import com.blockchaintp.daml.stores.layers.RetryingStore;
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

@SuppressWarnings({ "unchecked", "rawtypes" })
class RetryingStoreTest {
  @Test
  void get_retries_configured_number_of_store_read_exceptions() throws StoreReadException {
    var store = mock(Store.class);
    var retrying = new RetryingStore(new RetryingConfig(), store);

    when(store.get(any(Key.class))).thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build())).thenReturn(Optional.of(Value.of("stuff")));

    var retMap = new HashMap<>();
    retMap.put(Key.of("stuff"), Value.of("stuff"));
    when(store.get(any(List.class))).thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build())).thenReturn(retMap);

    /// Scalar get
    Assertions.assertEquals(Optional.of(Value.of("stuff")), retrying.get(Key.of("")));

    /// List get
    Assertions.assertEquals(retMap, retrying.get(Arrays.asList(Key.of("stuff"))));
  }

  @Test
  void get_eventually_fails_with_a_store_read_exception() throws StoreReadException {
    var store = mock(Store.class);
    var retrying = new RetryingStore(new RetryingConfig(), store);

    when(store.get(any(Key.class))).thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build())).thenReturn(Optional.of(Value.of("stuff")));

    when(store.get(any(List.class))).thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build()))
        .thenThrow(new StoreReadException(S3Exception.builder().build())).thenReturn(new HashMap<>());

    var getEx = Assertions.assertThrows(StoreReadException.class, () -> retrying.get(Key.of("")));

    var getMultipleEx = Assertions.assertThrows(StoreReadException.class, () -> retrying.get(Arrays.asList()));

    /// Check we did not double wrap
    Assertions.assertInstanceOf(S3Exception.class, getEx.getCause());

    Assertions.assertInstanceOf(S3Exception.class, getMultipleEx.getCause());
  }

  @Test
  void put_retries_configured_number_of_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new RetryingStore(new RetryingConfig(), store);

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store)
        .put(any(Key.class), any(Value.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store).put(any(List.class));

    /// Scalar put
    Assertions.assertDoesNotThrow(() -> retrying.put(Key.of(""), Value.of("")));

    /// List put
    Assertions.assertDoesNotThrow(() -> retrying.put(Arrays.asList()));
  }

  @Test
  void put_eventually_fails_with_a_store_write_exceptions() throws StoreWriteException {
    var store = mock(Store.class);
    var retrying = new RetryingStore(new RetryingConfig(), store);

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store)
        .put(any(Key.class), any(Value.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(store).put(any(List.class));

    /// Scalar put
    var putEx = Assertions.assertThrows(StoreWriteException.class, () -> retrying.put(Key.of(""), Value.of("")));

    /// List put
    var putMultipleEx = Assertions.assertThrows(StoreWriteException.class, () -> retrying.put(Arrays.asList()));

    /// Check we did not double wrap
    Assertions.assertInstanceOf(S3Exception.class, putEx.getCause());

    Assertions.assertInstanceOf(S3Exception.class, putMultipleEx.getCause());

  }
}
