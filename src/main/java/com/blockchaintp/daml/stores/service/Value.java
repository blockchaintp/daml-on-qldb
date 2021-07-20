/*
 * Copyright 2021 Blockchain Technology Partners
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

import java.util.function.Function;

/**
 * Wraps a value of type T.
 *
 * @param <T>
 *          the type to wrap
 */
public final class Value<T> extends Opaque<T> {

  /**
   * Return a key of the provided values type, with value.
   *
   * @param val
   *          the value to be wrapped
   */
  public Value(final T val) {
    super(val);
  }

  /**
   * Return a key of the provided values type, with value.
   *
   * @param <V>
   *          Type of Key
   * @param val
   *          the value of the key
   * @return the fully baked value
   */
  public static <V> Value<V> of(final V val) {
    return new Value<>(val);
  }

  /**
   * Map Value of one type into another.
   *
   * @param <O>
   *          the type of the output value
   * @param f
   *          function to transform value of type T into O
   * @return a Value of type O
   */
  public <O> Value<O> map(final Function<T, O> f) {
    return new Value<>(f.apply(this.value()));
  }
}
