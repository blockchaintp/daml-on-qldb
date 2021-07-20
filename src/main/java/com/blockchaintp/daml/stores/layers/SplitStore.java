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
package com.blockchaintp.daml.stores.layers;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

/**
 * A Store that keeps its values in a blob store which is keyed by the hash of the value. That
 * original key is stored along with that hash in the TransactionLog.
 */
public final class SplitStore implements TransactionLog<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<ByteString, ByteString> txLog;
  private final Store<String, byte[]> blobs;
  private final StoreReader<ByteString, ByteString> reader;
  private final UnaryOperator<byte[]> hashFn;

  /**
   * Constructs a new SplitStore.
   *
   * @param s3Index
   *          whether to allow fetching from the blob store if the value has been verified
   * @param indexReader
   *          a reader for the index, verified or not
   * @param txlog
   *          the TransactionLog to use
   * @param blobStore
   *          the blob store to use
   * @param hashingFn
   *          the hash function to use
   */
  public SplitStore(final boolean s3Index, final StoreReader<ByteString, ByteString> indexReader,
      final TransactionLog<ByteString, ByteString> txlog, final Store<String, byte[]> blobStore,
      final UnaryOperator<byte[]> hashingFn) {
    this.writeS3Index = s3Index;
    this.reader = indexReader;
    this.txLog = txlog;
    this.blobs = blobStore;
    this.hashFn = hashingFn;
  }

  @Override
  public void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {
    var bytes = value.toNative().toByteArray();
    var hash = hashFn.apply(bytes);

    var hexKey = DatatypeConverter.printHexBinary(hash);

    if (writeS3Index) {
      writeIndexedBlob(key, bytes, hash, hexKey);
    } else {
      writeBlob(bytes, hexKey);
    }

    txLog.put(key, Value.of(ByteString.copyFrom(hash)));
  }

  private void writeBlob(final byte[] bytes, final String hexKey) throws StoreWriteException {
    blobs.put(Key.of(hexKey), Value.of(bytes));
  }

  private void writeIndexedBlob(final Key<ByteString> key, final byte[] bytes, final byte[] hash, final String hexKey)
      throws StoreWriteException {
    blobs.put(
        Arrays.asList(new AbstractMap.SimpleEntry<>(Key.of(hexKey), Value.of(bytes)), new AbstractMap.SimpleEntry<>(
            Key.of(String.format("index/%s", key.toNative().toStringUtf8())), Value.of(hash))));
  }

  @Override
  public void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    for (var kv : listOfPairs) {
      this.put(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void sendEvent(final String topic, final String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(final List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    return reader.get(key);
  }

  @Override
  public Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys) throws StoreReadException {
    return reader.get(listOfKeys);
  }
}
