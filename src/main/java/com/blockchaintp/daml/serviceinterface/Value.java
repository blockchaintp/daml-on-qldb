package com.blockchaintp.daml.serviceinterface;

import java.util.function.Function;

/**
 * Wraps a value of type T.
 * @param <T> the type to wrap
 */
public final class Value<T> extends Opaque<T> {

  /**
   * Return a key of the provided values type, with value.
   * @param <V> Type of Key
   * @param val the value of the key
   * @return the fully baked value
   */
  public static <V> Value<V> of(final V val) {
    return new Value<>(val);
  }

  /**
   * Return a key of the provided values type, with value.
   * @param val the value to be wrapped
   */
  public Value(final T val) {
    super(val);
  }

  /**
   * Map Value of one type into another.
   * @param <O> the type of the output value
   * @param f   function to transform value of type T into O
   * @return a Value of type O
   */
  public <O> Value<O> map(final Function<T, O> f) {
    return new Value<>(f.apply(this.value()));
  }
}
