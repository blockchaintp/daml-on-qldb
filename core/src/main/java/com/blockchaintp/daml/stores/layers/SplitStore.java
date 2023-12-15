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
package com.blockchaintp.daml.stores.layers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import io.vavr.Tuple;

/**
 * A Store that keeps its values in a blob store which is keyed by the hash of the value. That
 * original key is stored along with that hash in the TransactionLog.
 */
public final class SplitStore implements Store<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<ByteString, ByteString> refStore;
  private final Store<ByteString, ByteString> blobs;
  private final StoreReader<ByteString, ByteString> reader;
  private final UnaryOperator<byte[]> hashFn;

  /**
   *
   * @param refstore
   * @param blobStore
   * @return A builder for a splitstore composed from refstore and blobstore
   */
  public static SplitStoreBuilder fromStores(final Store<ByteString, ByteString> refstore,
      final Store<ByteString, ByteString> blobStore) {
    return new SplitStoreBuilder(refstore, blobStore);
  }

  /**
   * Constructs a new SplitStore.
   *
   * @param s3Index
   *          whether to allow fetching from the blob store if the value has been verified
   * @param indexReader
   *          a reader for the index, verified or not
   * @param refstore
   *          the reference store to use
   * @param blobStore
   *          the blob store to use
   * @param hashingFn
   *          the hash function to use
   */
  public SplitStore(final boolean s3Index, final StoreReader<ByteString, ByteString> indexReader,
      final Store<ByteString, ByteString> refstore, final Store<ByteString, ByteString> blobStore,
      final UnaryOperator<byte[]> hashingFn) {
    this.writeS3Index = s3Index;
    this.reader = indexReader;
    this.refStore = refstore;
    this.blobs = blobStore;
    this.hashFn = hashingFn;
  }

  @Override
  public void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {
    put(List.of(Map.entry(key, value)));
  }

  /**
   * Form an index key to directly access the object.
   *
   * @param key
   * @param hashFn
   * @return An index key
   */
  public static ByteString indexKey(final ByteString key, final UnaryOperator<byte[]> hashFn) {
    return ByteString
        .copyFromUtf8(String.format("index/%s", DatatypeConverter.printHexBinary(hashFn.apply(key.toByteArray()))));
  }

  @Override
  public void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    var hashes = listOfPairs.stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kv -> Tuple.of(kv.getValue().toNative(),
            ByteString.copyFrom(hashFn.apply(kv.getValue().toNative().toByteArray())), kv.getValue())));

    if (writeS3Index) {
      blobs.put(hashes.values().stream().map(
          v -> Arrays.asList(Map.entry(Key.of(v._2), v._3), Map.entry(Key.of(indexKey(v._1, hashFn)), Value.of(v._2))))
          .flatMap(Collection::stream).collect(Collectors.toList()));
    } else {
      blobs.put(hashes.values().stream().map(v -> Map.entry(Key.of(v._2), v._3)).collect(Collectors.toList()));
    }

    refStore.put(hashes.entrySet().stream().map(kv -> Map.entry(kv.getKey(), Value.of(kv.getValue()._2)))
        .collect(Collectors.toList()));

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
