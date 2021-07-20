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
import java.util.Map;
import java.util.Optional;

import com.blockchaintp.daml.stores.exception.StoreReadException;

/**
 * A read interface for a K/V store.
 *
 * @param <K>
 *          the type of the keys
 * @param <V>
 *          the type of the values
 */
public interface StoreReader<K, V> {

  // TODO seems like busy work for the user of the interface to wrap in a Key
  // TODO might be useful to have a getOrDefault here as well

  /**
   * Return an Option of the value behind the Key in the K/V store.
   *
   * @param key
   *          the key
   * @return Option of the value
   * @throws StoreReadException
   *           error reading the store
   */
  Optional<Value<V>> get(Key<K> key) throws StoreReadException;

  // TODO seems to me this should return a map of Options, checking the map for
  // presence of keys can be tedious and repetitive

  /**
   * Return a list of values corresponding to the provided list of Keys.
   *
   * @param listOfKeys
   *          the list of Keys to fetch
   * @return a amp of K/V pairs
   * @throws StoreReadException
   *           error reading the store
   */
  Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys) throws StoreReadException;
}
