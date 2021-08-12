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
package com.blockchaintp.utility;

import io.vavr.CheckedFunction0;
import scala.Function0;

/**
 * Function utilities.
 */
public final class Functions {

  /**
   * Run this function unchecked.
   *
   * @param fn
   * @param <T>
   * @return The unchecked result of this function.
   */
  public static <T> Function0<T> uncheckFn(final CheckedFunction0<T> fn) {
    return () -> fn.unchecked().apply();
  }

  private Functions() {
  }
}
