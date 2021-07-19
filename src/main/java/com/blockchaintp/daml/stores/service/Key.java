package com.blockchaintp.daml.stores.service;

import java.util.function.Function;

/**
 * An Opaque meant semantically to be used as a key in some K/V store.
 * @param <T> the type of the key
 */
public final class Key<T> extends Opaque<T> {

  /**
   * Return a key of the provided values type, with value.
   * @param <K> Type of Key
   * @param val the value of the key
   * @return the fully baked value
   */
  public static <K> Key<K> of(final K val) {
    return new Key<>(val);
  }

  /**
   * Return a key of the provided values type, with value.
   * @param val the value of the key
   */
  public Key(final T val) {
    super(val);
  }

  /**
   * Map key of one type into another.
   * @param <O> the type of the output key
   * @param f function to transform value of type T into O
   * @return a key of type O
   */
  public <O> Key<O> map(final Function<T, O> f) {
    return new Key<>(f.apply(value()));
  }
}
