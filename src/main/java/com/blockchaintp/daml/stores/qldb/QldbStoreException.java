package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonValue;

import java.io.IOException;

public class QldbStoreException extends IOException {
  public QldbStoreException(String message) {
    super(message);
  }

  public static QldbStoreException invalidSchema(IonValue value) {
    return new QldbStoreException(String.format("Invalid result from qldb %s", value.toPrettyString()));
  }
}
