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
   * @param data
   * @return The transaction log id of the uncommitted transaction
   */
  K begin(V data);

  /**
   * Commit the transaction with this identifier.
   *
   * @param txId
   * @return the position in the log of the transaction.
   */
  I commit(K txId);

  /**
   * Abort the transaction with this identifier.
   *
   * @param txId
   */
  void abort(K txId);
}
