package com.blockchaintp.daml.serviceinterface;

import java.util.function.Function;

public final class Value<T> extends Opaque<T> {

  public static <V> Value<V> of(V val) {
    return new Value<>(val);
  }

  public Value(T value) {
    super(value);
  }

  public <I> Value<I> map(Function<T, I> f) {
    return new Value<>(f.apply(this.value));
  }
}
