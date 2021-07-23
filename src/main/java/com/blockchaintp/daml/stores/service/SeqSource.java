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

import java.util.List;

/**
 * A source of ordered values of I.
 *
 * @param <I>
 */
public interface SeqSource<I> {
  /**
   *
   * @return the next I, without consuming it
   */
  I peekNext();

  /**
   *
   * @return the next I, consuming it
   */
  I takeNext();

  /**
   * A non comsumed sequence of I with the specified size.
   *
   * @param size
   * @return An ordered sequence
   */
  List<I> peekRange(long size);

  /**
   * A comsumed sequence of I with the specified size.
   *
   * @param size
   * @return An ordered sequence
   */
  List<I> takeRange(long size);
}
