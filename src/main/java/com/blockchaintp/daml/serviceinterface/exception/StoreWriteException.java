package com.blockchaintp.daml.serviceinterface.exception;

/**
 * Represents a write failure of some sort to the store.
 */
public class StoreWriteException extends StoreException {

  /**
   * Write exception with cause and error message.
   * @param error the error message
   * @param cause the originating exception
   */
  public StoreWriteException(final String error, final Throwable cause) {
    super(error, cause);
  }

  /**
   * A write exception with an originating cause
   * @param cause the cause
   */
  public StoreWriteException(final Throwable cause) {
    super(cause);
  }
}
