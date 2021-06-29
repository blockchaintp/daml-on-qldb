package com.blockchaintp.daml.serviceinterface.exception;

public class StoreReadException extends StoreException {
  public StoreReadException(String error, Throwable cause) {
    super(error, cause);
  }

    public StoreReadException(Throwable e) {
      super(e);
    }
}
