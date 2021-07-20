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

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.service.Key;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.Executor;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.nio.charset.Charset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class QldbStoreTest {

  @Test
  void session_acquisition_failures_raises_reasonable_exceptions() {
    var closedDriver = mock(QldbDriver.class);
    when(closedDriver.execute(any(Executor.class))).thenThrow(QldbDriverException.create(Errors.DRIVER_CLOSED.get()));

    var closedStore = new QldbStore(closedDriver, "");

    Assertions.assertThrows(StoreReadException.class,
        () -> closedStore.get(Key.of(ByteString.copyFrom("identity", Charset.defaultCharset()))));
  }
}
