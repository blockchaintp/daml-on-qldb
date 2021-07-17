package com.blockchaintp.daml.stores.exception;

/**
 * Represents a read failure of some sort to the store.
 */
public class StoreReadException extends StoreException {

  /**
   * Read exception with cause and error message.
   * @param error the error message
   * @param cause the originating exception
   */
  public StoreReadException(final String error, final Throwable cause) {
    super(error, cause);
  }

  /**
   * A read exception with an originating cause.
   * @param cause the cause
   */
  public StoreReadException(final Throwable cause) {
    super(cause);
  }

}
