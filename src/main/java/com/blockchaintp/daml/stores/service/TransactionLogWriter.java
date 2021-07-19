package com.blockchaintp.daml.stores.service;

import com.blockchaintp.daml.stores.exception.StoreWriteException;

import java.util.List;
import java.util.Map;

/**
 * A TransactionLogWriter is a StoreWriter that also supports the sending of events.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public interface TransactionLogWriter<K, V> extends StoreWriter<K, V> {

  /**
   * Send an event on the specified topic with the specified payload.
   *
   * @param topic the topic to publish the event
   * @param data  the data payload/
   * @throws StoreWriteException an error writing to the store
   */
  void sendEvent(String topic, String data) throws StoreWriteException;

  /**
   * Send an event on multiple topics with multiple payloads.
   *
   * @param listOfTopicDataPairs a map where the key is the topic and the value is the data payload
   * @throws StoreWriteException an error writing to the store
   */
  void sendEvent(List<Map.Entry<String, String>> listOfTopicDataPairs) throws StoreWriteException;

}
