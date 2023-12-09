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
package com.blockchaintp.daml.stores.layers;

import java.util.function.Function;

/**
 * Provide a method of converting bidrectionally between T1 and T2.
 *
 * @param <T1>
 * @param <T2>
 */
public final class Bijection<T1, T2> {
  private final Function<T1, T2> convertTo;
  private final Function<T2, T1> convertFrom;

  private Bijection(final Function<T1, T2> theTo, final Function<T2, T1> theFrom) {
    convertTo = theTo;
    convertFrom = theFrom;
  }

  /**
   *
   * @param theTo
   * @param theFrom
   * @param <T3>
   * @param <T4>
   * @return A birectional conversion between two types.
   */
  public static <T3, T4> Bijection<T3, T4> of(final Function<T3, T4> theTo, final Function<T4, T3> theFrom) {
    return new Bijection<>(theTo, theFrom);
  }

  /**
   *
   * @param from
   * @return A converted result.
   */
  public T2 to(final T1 from) {
    return convertTo.apply(from);
  }

  /**
   *
   * @return A converted result.
   * @param to
   * @return
   */
  public T1 from(final T2 to) {
    return convertFrom.apply(to);
  }

  /**
   *
   * @param <T3>
   * @return No conversion needed.
   */
  public static <T3> Bijection<T3, T3> identity() {
    return new Bijection<>(x -> x, x -> x);
  }
}
