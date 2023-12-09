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

import java.util.Optional;
import java.util.stream.Stream;

import com.blockchaintp.daml.stores.exception.StoreReadException;

import io.vavr.Tuple3;

/**
 *
 * @param <I>
 *          Sequence type
 * @param <K>
 *          Log entry identifier type
 * @param <V>
 *          Log entry type
 */
public interface TransactionLogReader<I, K, V> {
  /**
   * Stream committed log entries starting at offset.
   *
   * @param startExclusive
   * @param endInclusive
   * @return A stream of comitted log entires.
   */
  Stream<Tuple3<I, K, V>> from(I startExclusive,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<I> endInclusive) throws StoreReadException;

  /**
   *
   * @return The offset of the last written entry.
   */
  Optional<I> getLatestOffset();
}
