package com.blockchaintp.daml.exception;

import java.security.NoSuchAlgorithmException;

public class NoSHA512SupportException extends RuntimeException {
  public NoSHA512SupportException(final NoSuchAlgorithmException cause) {
    super("No SHA-512 support available", cause);
  }
}
