package com.blockchaintp.daml.serviceinterface;

import java.io.Serializable;
import java.util.Objects;

public abstract class Opaque<T> implements Serializable {
  protected final T value;

  protected Opaque(T value) {
    this.value = value;
  }

  public T toNative() {
    return this.value;
  }


  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Opaque<?> opaque = (Opaque<?>) o;
    return value.equals(opaque.value);
  }
}
