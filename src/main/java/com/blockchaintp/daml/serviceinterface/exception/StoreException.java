package com.blockchaintp.daml.serviceinterface.exception;

import java.io.IOException;

public class StoreException extends IOException {
  protected StoreException(String error, Throwable cause) {
    super(error, cause);
  }

  public StoreException(Throwable e) {
    super(e);
  }
}
