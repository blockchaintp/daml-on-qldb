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
package com.blockchaintp.daml.stores.layers;

import com.blockchaintp.daml.stores.exception.StoreException;

import io.vavr.Function1;

/**
 * Wrap an unchecked runnable with checking and an exception wrap.
 *
 * @param <E>
 */
public final class WrapRunnable<E extends StoreException> {
  private final Runnable fn;
  private final Function1<Exception, E> wrap;

  /**
   *
   * @param theFn
   * @param theWrap
   */
  public WrapRunnable(final Runnable theFn, final Function1<Exception, E> theWrap) {
    fn = theFn;
    wrap = theWrap;
  }

  /**
   *
   * @param f
   * @param wrap
   * @param <EX>
   * @return A wrapped function.
   */
  public static <EX extends StoreException> WrapRunnable<EX> of(final Runnable f, final Function1<Exception, EX> wrap) {
    return new WrapRunnable<>(f, wrap);
  }

  /**
   * Exectute the wrapped runnable.
   *
   * @throws E
   */
  public void run() throws E {
    try {
      fn.run();
    } catch (RuntimeException e) {
      throw wrap.apply(e);
    } catch (Exception e) {
      if (!(e instanceof StoreException)) {
        throw wrap.apply(e);
      }

      throw e;
    }

  }
}
