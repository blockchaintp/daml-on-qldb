/*
 * Copyright © 2023 Paravela Limited
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

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.RetryingTransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLog;
import io.vavr.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryingTransactionLogTest {

  class InnerException extends Exception {
  }

  @Test
  void transaction_log_operations_are_retried() throws StoreWriteException {
    var log = mock(TransactionLog.class);
    var retrying = new RetryingTransactionLog(3, log);

    when(log.begin(Optional.empty())).thenThrow(new StoreWriteException(new InnerException()))
        .thenThrow(new StoreWriteException(new InnerException())).thenReturn(Tuple.of(UUID.randomUUID(), 0));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doNothing().when(log).sendEvent(any(UUID.class), any(String.class));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doReturn(Long.valueOf(0L)).when(log).commit(any(UUID.class));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doNothing().when(log).abort(any(UUID.class));

    Assertions.assertDoesNotThrow(() -> retrying.begin(Optional.empty()));
    Assertions.assertDoesNotThrow(() -> retrying.commit(UUID.randomUUID()));
    Assertions.assertDoesNotThrow(() -> retrying.sendEvent(UUID.randomUUID(), ""));
    Assertions.assertDoesNotThrow(() -> retrying.abort(UUID.randomUUID()));

  }

  @Test
  void transaction_log_write_operations_eventually_fail() throws StoreWriteException {

    var log = mock(TransactionLog.class);
    var retrying = new RetryingTransactionLog(3, log);

    when(log.begin(Optional.empty())).thenThrow(new StoreWriteException(new InnerException()))
        .thenThrow(new StoreWriteException(new InnerException()))
        .thenThrow(new StoreWriteException(new InnerException()))
        .thenThrow(new StoreWriteException(new InnerException())).thenReturn(Tuple.of(UUID.randomUUID(), 0));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doNothing().when(log).sendEvent(any(UUID.class), any(String.class));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doReturn(Long.valueOf(0L)).when(log).commit(any(UUID.class));

    doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doThrow(new StoreWriteException(new InnerException())).doThrow(new StoreWriteException(new InnerException()))
        .doNothing().when(log).abort(any(UUID.class));

    Assertions.assertInstanceOf(InnerException.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.begin(Optional.empty())).getCause());

    Assertions.assertInstanceOf(InnerException.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.commit(UUID.randomUUID())).getCause());

    Assertions.assertInstanceOf(InnerException.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.sendEvent(UUID.randomUUID(), "")).getCause());

    Assertions.assertInstanceOf(InnerException.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.abort(UUID.randomUUID())).getCause());

  }
}
