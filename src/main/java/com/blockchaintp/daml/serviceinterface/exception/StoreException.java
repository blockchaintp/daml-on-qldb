package com.blockchaintp.daml.serviceinterface.exception;

import java.io.IOException;

/**
 * Represents a generic exception in the underlying store.
 */
public class StoreException extends IOException {

  /**
   * An exception with cause and error message.
   * @param error the error message
   * @param cause the originating exception
   */
  protected StoreException(final String error, final Throwable cause) {
    super(error, cause);
  }

  /**
   * An exception with an originating cause.
   * @param cause the cause
   */
  public StoreException(final Throwable cause) {
    super(cause);
  }
}
