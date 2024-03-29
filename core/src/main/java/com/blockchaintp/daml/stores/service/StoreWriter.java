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

import java.util.List;
import java.util.Map;

import com.blockchaintp.daml.stores.exception.StoreWriteException;

/**
 * Represents a K/V store which we can write to.
 *
 * @param <K>
 *          the type of the keys
 * @param <V>
 *          the type of the values
 */
public interface StoreWriter<K, V> {

  /**
   * Put the specified value at the specified key.
   *
   * @param key
   *          the key
   * @param value
   *          the value
   * @throws StoreWriteException
   *           error writing to store
   */
  void put(Key<K> key, Value<V> value) throws StoreWriteException;

  /**
   * Put a list of K/V pairs t the store.
   *
   * @param listOfPairs
   *          the list of K/V pairs to put
   * @throws StoreWriteException
   *           error writing to the store
   */
  void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException;

}
