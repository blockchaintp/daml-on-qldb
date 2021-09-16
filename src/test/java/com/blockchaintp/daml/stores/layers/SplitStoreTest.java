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
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class SplitStoreTest {
  @Test
  void unindexed_put_enters_hash_into_txlog_after_storing_in_blobstore()
      throws StoreWriteException, StoreReadException {
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var splitStore = new SplitStore(false, mock(StoreReader.class), txLog, blobStore, bytes -> new byte[] { 'o', 'k' });

    splitStore.put(Key.of(ByteString.copyFromUtf8("DamlKey")), Value.of(ByteString.copyFromUtf8("CONTENT")));

    // txlog should contain hash
    Assertions.assertEquals(ByteString.copyFromUtf8("ok"),
        txLog.get(Key.of(ByteString.copyFromUtf8("DamlKey"))).get().toNative());

    // blobstore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFromUtf8("CONTENT").toByteArray(),
        blobStore.get(Key.of(ByteString.copyFromUtf8("ok"))).get().toNative().toByteArray());

  }

  @Test
  void indexed_put_enters_hash_into_txlog_after_storing_in_blobStore_and_also_stores_index()
      throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();

    var splitStore = new SplitStore(true, mock(StoreReader.class), txLog, blobStore, bytes -> new byte[] { 'o', 'k' });

    splitStore.put(Key.of(ByteString.copyFromUtf8("DamlKey")), Value.of(ByteString.copyFromUtf8("CONTENT")));

    // txlog should contain hash
    Assertions.assertEquals(ByteString.copyFrom("ok", Charset.defaultCharset()),
        txLog.get(Key.of(ByteString.copyFromUtf8("DamlKey"))).get().toNative());

    // blobStore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFromUtf8("CONTENT").toByteArray(),
        blobStore.get(Key.of(ByteString.copyFromUtf8("ok"))).get().toNative().toByteArray());

    // blobStore Index should contain blob hash
    Assertions.assertArrayEquals(ByteString.copyFromUtf8("ok").toByteArray(),
        blobStore.get(Key.of(ByteString.copyFromUtf8("index/6F6B"))).get().toNative().toByteArray());
  }

  @Test
  void verified_reader_reads_hash_from_txlog_then_looks_up_as_key_in_blobstore() throws StoreReadException {

    var refStore = mock(Store.class);
    var blobStore = mock(Store.class);
    var verified = new VerifiedReader(refStore, blobStore);

    var txStoreResult = ByteString.copyFrom("ok", Charset.defaultCharset());

    when(refStore.get(Arrays.asList(Key.of(ByteString.copyFromUtf8("DamlKey"))))).thenReturn(
        Map.of(Key.of(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset()))), Value.of(txStoreResult)));
    when(blobStore.get(Arrays.asList(Key.of(ByteString.copyFromUtf8("ok")))))
        .thenReturn(Map.of(Key.of(ByteString.copyFromUtf8("ok")), Value.of(ByteString.copyFromUtf8("x"))));

    Optional<Value<ByteString>> blobStoreVal = verified.get(Key.of(ByteString.copyFromUtf8("DamlKey")));

    Assertions.assertEquals(ByteString.copyFromUtf8("x"), blobStoreVal.get().toNative());
  }
}
