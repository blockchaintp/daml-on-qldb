package com.blockchaintp.daml.serviceinterface.exception;

public class StoreWriteException extends StoreException {
  protected StoreWriteException(String error, Throwable cause) {
    super(error, cause);
  }
}
