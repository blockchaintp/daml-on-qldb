package com.blockchaintp.exception;

import java.security.NoSuchAlgorithmException;

/**
 * A serious error that indicates that the JVM environment does not support SHA-512.
 */
public class NoSHA512SupportException extends RuntimeException {

  /**
   * Exception with cause.
   *
   * @param cause
   */
  public NoSHA512SupportException(final NoSuchAlgorithmException cause) {
    super("No SHA-512 support available", cause);
  }
}
