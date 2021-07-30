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

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.RetryingConfig;
import com.blockchaintp.daml.stores.layers.RetryingTransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryingTransactionLogTest {
  @Test
  void transaction_log_operations_are_retried() throws StoreWriteException {
    var log = mock(TransactionLog.class);
    var retrying = new RetryingTransactionLog(new RetryingConfig(), log);

    when(log.begin()).thenThrow(new StoreWriteException(S3Exception.builder().build()))
        .thenThrow(new StoreWriteException(S3Exception.builder().build())).thenReturn(UUID.randomUUID());

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(log)
        .sendEvent(any(UUID.class), any(String.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doReturn(Long.valueOf(0L)).when(log)
        .commit(any(UUID.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(log).abort(any(UUID.class));

    Assertions.assertDoesNotThrow(() -> retrying.begin());
    Assertions.assertDoesNotThrow(() -> retrying.commit(UUID.randomUUID()));
    Assertions.assertDoesNotThrow(() -> retrying.sendEvent(UUID.randomUUID(), ""));
    Assertions.assertDoesNotThrow(() -> retrying.abort(UUID.randomUUID()));

  }

  @Test
  void transaction_log_write_operations_eventually_fail() throws StoreWriteException {

    var log = mock(TransactionLog.class);
    var retrying = new RetryingTransactionLog(new RetryingConfig(), log);

    when(log.begin()).thenThrow(new StoreWriteException(S3Exception.builder().build()))
        .thenThrow(new StoreWriteException(S3Exception.builder().build()))
        .thenThrow(new StoreWriteException(S3Exception.builder().build()))
        .thenThrow(new StoreWriteException(S3Exception.builder().build())).thenReturn(UUID.randomUUID());

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(log)
        .sendEvent(any(UUID.class), any(String.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doReturn(Long.valueOf(0L)).when(log)
        .commit(any(UUID.class));

    doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build()))
        .doThrow(new StoreWriteException(S3Exception.builder().build())).doNothing().when(log).abort(any(UUID.class));

    Assertions.assertInstanceOf(S3Exception.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.begin()).getCause());

    Assertions.assertInstanceOf(S3Exception.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.commit(UUID.randomUUID())).getCause());

    Assertions.assertInstanceOf(S3Exception.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.sendEvent(UUID.randomUUID(), "")).getCause());

    Assertions.assertInstanceOf(S3Exception.class,
        Assertions.assertThrows(StoreWriteException.class, () -> retrying.abort(UUID.randomUUID())).getCause());

  }
}
