package com.blockchaintp.daml.serviceinterface;

import java.util.function.Function;

public final class Key<T> extends Opaque<T> {

  public static <K> Key<K> of(K val) {
    return new Key<>(val);
  }

  public Key(T value) {
    super(value);
  }

  public <I> Key<I> map(Function<T, I> f) {
    return new Key<>(f.apply(this.value));
  }
}
