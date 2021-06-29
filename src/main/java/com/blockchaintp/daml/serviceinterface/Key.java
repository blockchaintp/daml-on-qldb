package com.blockchaintp.daml.serviceinterface;

public final class Key<T> extends Opaque<T> {
  public Key(T value) {
    super(value);
  }
}
