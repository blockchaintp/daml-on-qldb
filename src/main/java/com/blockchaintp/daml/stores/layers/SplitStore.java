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

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * A Store that keeps its values in a blob store which is keyed by the hash of the value. That
 * original key is stored along with that hash in the TransactionLog.
 */
public class SplitStore implements TransactionLog<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<ByteString, ByteString> qldb;
  private final Store<String, byte[]> s3;
  private final StoreReader<ByteString, ByteString> reader;
  private final UnaryOperator<byte[]> hashFn;

  /**
   * Constructs a new SplitStore.
   *
   * @param s3Index
   *          whether to allow fetching from the blob store if the value has been verified
   * @param indexReader
   *          a reader for the index, verified or not
   * @param txLog
   *          the TransactionLog to use
   * @param blobStore
   *          the blob store to use
   * @param hashingFn
   *          the hash function to use
   */
  public SplitStore(final boolean s3Index, final StoreReader<ByteString, ByteString> indexReader,
      final TransactionLog<ByteString, ByteString> txLog, final Store<String, byte[]> blobStore,
      final UnaryOperator<byte[]> hashingFn) {
    this.writeS3Index = s3Index;
    this.reader = indexReader;
    this.qldb = txLog;
    this.s3 = blobStore;
    this.hashFn = hashingFn;
  }

  @Override
  public void put(Key<ByteString> key, Value<ByteString> value) throws StoreWriteException {
    var bytes = value.toNative().toByteArray();
    var hash = hashFn.apply(bytes);

    var hexKey = DatatypeConverter.printHexBinary(hash);

    if (writeS3Index) {
      writeIndexedBlob(key, bytes, hash, hexKey);
    } else {
      writeBlob(bytes, hexKey);
    }

    qldb.put(key, Value.of(ByteString.copyFrom(hash)));
  }

  private void writeBlob(byte[] bytes, String hexKey) throws StoreWriteException {
    s3.put(new Key<>(hexKey), new Value<>(bytes));
  }

  private void writeIndexedBlob(Key<ByteString> key, byte[] bytes, byte[] hash, String hexKey)
      throws StoreWriteException {
    s3.put(Arrays.asList(new AbstractMap.SimpleEntry<>(new Key<>(hexKey), new Value<>(bytes)),
        new AbstractMap.SimpleEntry<>(new Key<>(String.format("index/%s", key.toNative().toStringUtf8())),
            new Value<>(hash))));
  }

  @Override
  public void put(List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    for (var kv : listOfPairs) {
      this.put(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {
    return reader.get(key);
  }

  @Override
  public Map<Key<ByteString>, Value<ByteString>> get(List<Key<ByteString>> listOfKeys) throws StoreReadException {
    return reader.get(listOfKeys);
  }
}
