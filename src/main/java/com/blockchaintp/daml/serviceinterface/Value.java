package com.blockchaintp.daml.serviceinterface;

public final class Value<T> extends Opaque<T> {
  public Value(T value) {
    super(value);
  }
}
