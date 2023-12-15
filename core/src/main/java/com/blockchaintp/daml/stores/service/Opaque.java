/*
 * Copyright © 2023 Paravela Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.stores.service;

import java.util.Objects;

/**
 * An Opaque is a wrapper around a value for passing around.
 *
 * @param <T>
 *          the type to wrap
 */
public abstract class Opaque<T> {

  private final T value;

  protected Opaque(final T v) {
    this.value = v;
  }

  /**
   * This is used by children for equivalence tests.
   *
   * @return the internal comparable value
   */
  protected final T value() {
    return this.value;
  }

  /**
   * Returns the native value that was provided in construction.
   *
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
