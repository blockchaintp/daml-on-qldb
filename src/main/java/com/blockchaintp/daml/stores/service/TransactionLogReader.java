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

import java.util.Optional;

import io.reactivex.rxjava3.core.Observable;
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
   * @param offset
   *          - use None to start at the last commit
   * @return A stream of comitted log entires.
   */
  Observable<Tuple3<I, K, V>> from(Optional<I> offset);
}
