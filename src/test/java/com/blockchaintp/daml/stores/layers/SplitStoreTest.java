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

import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.SplitStore;
import com.blockchaintp.daml.stores.layers.VerifiedReader;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class SplitStoreTest {
  @Test
  void unindexed_put_enters_hash_into_txlog_after_storing_in_blobstore()
      throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<String, byte[]>();
    var splitStore = new SplitStore(false, mock(StoreReader.class), txLog, blobStore, bytes -> new byte[] { 'o', 'k' });

    splitStore.put(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
        Value.of(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));

    // txlog should contain hash
    Assertions.assertEquals(ByteString.copyFrom("ok", Charset.defaultCharset()),
        txLog.get(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset()))).get().toNative());

    // blobstore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
        blobStore.get(Key.of("6F6B")).get().toNative());

  }

  @Test
  void indexed_put_enters_hash_into_txlog_after_storing_in_blobStore_and_also_stores_index()
      throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<String, byte[]>();

    var splitStore = new SplitStore(true, mock(StoreReader.class), txLog, blobStore, bytes -> new byte[] { 'o', 'k' });

    splitStore.put(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
        Value.of(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));

    // txlog should contain hash
    Assertions.assertEquals(ByteString.copyFrom("ok", Charset.defaultCharset()),
        txLog.get(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset()))).get().toNative());

    // blobStore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
        blobStore.get(Key.of("6F6B")).get().toNative());

    // blobStore Index should contain blob hash
    Assertions.assertArrayEquals(new byte[] { 'o', 'k' }, blobStore.get(Key.of("index/DamlKey")).get().toNative());
  }

  @Test
  void verified_reader_reads_hash_from_txlog_then_looks_up_as_key_in_blobstore() throws StoreReadException {

    var refStore = mock(Store.class);
    var blobStore = mock(Store.class);
    var verified = new VerifiedReader(refStore, blobStore);

    var txStoreResult = ByteString.copyFrom("ok", Charset.defaultCharset());

    when(refStore.get(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset()))))
        .thenReturn(Optional.of(Value.of(txStoreResult)));
    when(blobStore.get(Key.of("6F6B"))).thenReturn(Optional.of(Value.of(new byte[] { 'x' })));

    Optional<Value<ByteString>> blobStoreVal = verified
        .get(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset())));

    Assertions.assertEquals(ByteString.copyFrom(new byte[] { 'x' }), blobStoreVal.get().toNative());
  }
}
