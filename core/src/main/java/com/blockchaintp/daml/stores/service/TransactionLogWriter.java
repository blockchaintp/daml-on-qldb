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

import com.blockchaintp.daml.stores.exception.StoreWriteException;

import io.vavr.Tuple2;

/**
 * A TransactionLogWriter is a StoreWriter that also supports the sending of events.
 *
 * @param <K>
 *          the type of the transaction log id
 *
 * @param <V>
 *          the type of the transaction value
 *
 * @param <I>
 *          the type of the transaction sequence
 *
 */
public interface TransactionLogWriter<K, V, I> {
  /**
   *
   * @return The transaction log id of the uncommitted transaction
   * @param id
   */
  Tuple2<K, I> begin(Optional<K> id) throws StoreWriteException;

  /**
   * Update the log entry for the id.
   *
   * @param id
   * @param data
   */
  void sendEvent(K id, V data) throws StoreWriteException;

  /**
   * Commit the transaction with this identifier.
   *
   * @param txId
   * @return the position in the log of the transaction.
   */
  I commit(K txId) throws StoreWriteException;

  /**
   * Abort the transaction with this identifier.
   *
   * @param txId
   */
  void abort(K txId) throws StoreWriteException;
}
