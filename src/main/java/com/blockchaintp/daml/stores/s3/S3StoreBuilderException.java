package com.blockchaintp.daml.stores.s3;

/**
 * RuntimeExceptions relating to the construction of S3Store objects.
 */
public class S3StoreBuilderException extends RuntimeException {
  /**
   * Exception with message and cause.
   *
   * @param message the message
   * @param cause   the originating exception
   */
  public S3StoreBuilderException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Exception with message.
   *
   * @param message the message
   */
  public S3StoreBuilderException(final String message) {
    super(message);
  }
}
