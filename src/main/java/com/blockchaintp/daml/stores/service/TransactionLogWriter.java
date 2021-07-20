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

import com.blockchaintp.daml.stores.exception.StoreWriteException;

import java.util.List;
import java.util.Map;

/**
 * A TransactionLogWriter is a StoreWriter that also supports the sending of events.
 *
 * @param <K>
 *          the type of the keys
 * @param <V>
 *          the type of the values
 */
public interface TransactionLogWriter<K, V> extends StoreWriter<K, V> {

  /**
   * Send an event on the specified topic with the specified payload.
   *
   * @param topic
   *          the topic to publish the event
   * @param data
   *          the data payload/
   * @throws StoreWriteException
   *           an error writing to the store
   */
  void sendEvent(String topic, String data) throws StoreWriteException;

  /**
   * Send an event on multiple topics with multiple payloads.
   *
   * @param listOfTopicDataPairs
   *          a map where the key is the topic and the value is the data payload
   * @throws StoreWriteException
   *           an error writing to the store
   */
  void sendEvent(List<Map.Entry<String, String>> listOfTopicDataPairs) throws StoreWriteException;

}
