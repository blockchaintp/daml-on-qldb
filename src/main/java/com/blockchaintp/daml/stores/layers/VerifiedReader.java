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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

/**
 * Checks QLDB contains the hash before reading value from s3.
 */
public class VerifiedReader implements StoreReader<ByteString, ByteString> {

  private final Store<ByteString, ByteString> refStore;
  private final Store<String, byte[]> blobStore;

  /**
   * Construct a VerifiedReader around the provided stores.
   *
   * @param refstore
   *          the reference store which masters the K->Hash map.
   * @param blobs
   *          the blob store which masters the Hash->Value map.
   */
  public VerifiedReader(final Store<ByteString, ByteString> refstore, final Store<String, byte[]> blobs) {
    this.refStore = refstore;
    this.blobStore = blobs;
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    return get(List.of(key)).values().stream().findFirst();
  }

  @Override
  public final Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys)
      throws StoreReadException {
    var refKeys = refStore.get(listOfKeys).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
        v -> DatatypeConverter.printHexBinary(v.getValue().toNative().toByteArray())));

    var values = blobStore.get(refKeys.values().stream().map(Key::of).collect(Collectors.toList()));

    var retMap = new HashMap<Key<ByteString>, Value<ByteString>>();

    for (var kv : refKeys.entrySet()) {
      if (values.containsKey((Key.of(kv.getValue())))) {
        retMap.put(kv.getKey(), values.get(Key.of(kv.getValue())).map(ByteString::copyFrom));
      }
    }

    return retMap;
  }
}
