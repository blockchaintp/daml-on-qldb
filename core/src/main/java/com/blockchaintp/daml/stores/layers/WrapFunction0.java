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

import com.blockchaintp.daml.stores.exception.StoreException;

import io.vavr.API;
import io.vavr.CheckedFunction0;
import io.vavr.Function0;
import io.vavr.Function1;

/**
 * A function interface that wraps a thrown exception from an unchecked inner function.
 *
 * @param <T>
 * @param <E>
 */
public final class WrapFunction0<T, E extends StoreException> {
  private final Function0<T> fn;
  private final Function1<Exception, E> wrap;

  /**
   * Wrap the function and supply a functor for the wrapping.
   *
   * @param f
   * @param theWrap
   */
  public WrapFunction0(final Function0<T> f, final Function1<Exception, E> theWrap) {
    this.fn = f;
    wrap = theWrap;
  }

  /**
   * Wrap a unchecked function with a checked wrapper and exception wrapping.
   *
   * @param inner
   * @param wrap
   * @param <R>
   * @param <E>
   * @return A wrapped function.
   */
  public static <R, E extends StoreException> WrapFunction0<R, E> of(final Function0<R> inner,
      final Function1<Exception, E> wrap) {
    return new WrapFunction0<>(inner, wrap);
  }

  /**
   * Wrap a checked function with a checked wrapper and exception wrapping.
   *
   * @param inner
   * @param wrap
   * @param <R>
   * @param <E>
   * @return A wrapped function.
   */
  public static <R, E extends StoreException> WrapFunction0<R, E> ofChecked(final CheckedFunction0<R> inner,
      final Function1<Exception, E> wrap) {
    return new WrapFunction0<>(API.unchecked(inner), wrap);
  }

  /**
   * Execute this function.
   *
   * @return the result.
   */
  public T apply() throws E {
    try {
      return fn.apply();
    } catch (RuntimeException e) {
      throw wrap.apply(e);
    } catch (Exception e) {
      if (!(e instanceof StoreException)) {
        throw wrap.apply(e);
      }
      throw e;
    }
  }

  /**
   * Apply, unchecked.
   *
   * @return the result.
   */
  public T unchecked() {
    return API.unchecked(this::apply).apply();
  }
}
