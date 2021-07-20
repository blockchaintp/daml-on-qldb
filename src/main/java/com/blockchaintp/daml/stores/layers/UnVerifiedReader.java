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

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

/**
 * Reads straight from s3 using an index.
 */
public class UnVerifiedReader implements StoreReader<ByteString, ByteString> {
  private final Store<String, byte[]> blobs;

  /**
   * Construct an unverified reader around the provided store.
   *
   * @param ourBlobs
   *          The underlying store
   */
  public UnVerifiedReader(final Store<String, byte[]> ourBlobs) {
    this.blobs = ourBlobs;
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {

    var s3Index = blobs.get(Key.of(String.format("index/%s", key.toNative().toStringUtf8())));

    if (s3Index.isPresent()) {
      return blobs.get(Key.of(DatatypeConverter.printHexBinary(s3Index.get().toNative())))
          .map(v -> v.map(ByteString::copyFrom));
    }

    return Optional.empty();
  }

  @Override
  public final Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys)
      throws StoreReadException {
    var map = new HashMap<Key<ByteString>, Value<ByteString>>();
    for (var k : listOfKeys) {
      var item = this.get(k);
      item.ifPresent(byteStringValue -> map.put(k, byteStringValue));
    }

    return map;
  }
}
