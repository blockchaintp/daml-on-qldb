package com.blockchaintp.daml.serviceinterface;

import java.io.Serializable;
import java.util.Objects;

/**
 * An Opaque is a wrapper around a value for passing around.
 * @param <T> the type to wrap
 */
public abstract class Opaque<T> implements Serializable {

  private final T value;

  protected Opaque(final T v) {
    this.value = v;
  }

  /**
   * This is used by children for equivalence tests.
   * @return the internal comparable value
   */
  protected final T value() {
    return this.value;
  }

  /**
   * Returns the native value that was provided in construction.
   * @return the native value
   */
  public final T toNative() {
    return this.value;
  }

  @Override
  public final String toString() {
    return value.toString();
  }

  @Override
  public final int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Opaque<?> opaque = (Opaque<?>) o;
    return value.equals(opaque.value);
  }
}
