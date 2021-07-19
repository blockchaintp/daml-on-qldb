package com.blockchaintp.daml.stores.qldb;

/**
 * An exception that is thrown when a QldbStoreBuilder is unable to build a
 * QldbStore.
 */
public class QldbStoreBuilderException extends RuntimeException {
  /**
   * Exception with message.
   *
   * @param message the message
   */
  public QldbStoreBuilderException(final String message) {
    super(message);
  }
}
