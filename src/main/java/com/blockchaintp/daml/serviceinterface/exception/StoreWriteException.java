package com.blockchaintp.daml.serviceinterface.exception;

public class StoreWriteException extends StoreException {
  public StoreWriteException(String error, Throwable cause) {
    super(error, cause);
  }

  public StoreWriteException(Throwable e) {
    super(e);
  }
}
