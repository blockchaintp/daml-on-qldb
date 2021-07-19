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
    when(closedDriver.execute(any(Executor.class)))
      .thenThrow(QldbDriverException.create(Errors.DRIVER_CLOSED.get()));

    var closedStore = new QldbStore(closedDriver, "");

    Assertions.assertThrows(StoreReadException.class,
      () -> closedStore.get(Key.of(ByteString.copyFrom("identity", Charset.defaultCharset()))));
  }
}
