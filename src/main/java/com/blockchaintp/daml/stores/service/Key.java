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
 * An Opaque meant semantically to be used as a key in some K/V store.
 *
 * @param <T>
 *          the type of the key
 */
public final class Key<T> extends Opaque<T> {

  /**
   * Return a key of the provided values type, with value.
   *
   * @param val
   *          the value of the key
   */
  public Key(final T val) {
    super(val);
  }

  /**
   * Return a key of the provided values type, with value.
   *
   * @param <K>
   *          Type of Key
   * @param val
   *          the value of the key
   * @return the fully baked value
   */
  public static <K> Key<K> of(final K val) {
    return new Key<>(val);
  }

  /**
   * Map key of one type into another.
   *
   * @param <O>
   *          the type of the output key
   * @param f
   *          function to transform value of type T into O
   * @return a key of type O
   */
  public <O> Key<O> map(final Function<T, O> f) {
    return Key.of(f.apply(value()));
  }
}
