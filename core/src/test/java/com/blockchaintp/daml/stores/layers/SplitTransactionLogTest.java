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

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.google.protobuf.ByteString;
import io.vavr.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

class SplitTransactionLogTest {

  @Test
  void commit_writes_records() throws StoreWriteException, StoreReadException {
    var txLog = new com.blockchaintp.daml.stores.StubTransactionLog();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();

    var splitLog = new SplitTransactionLog(txLog, blobStore, x -> new byte[] { 'o', 'k' });

    var uid = splitLog.begin(Optional.empty());

    Assertions.assertIterableEquals(Arrays.asList(uid), txLog.inProgress.keySet());

    splitLog.sendEvent(uid._1, ByteString.copyFromUtf8("test"));

    /// Should now have a hash in txLog and the bytes in the blobStore
    Assertions.assertArrayEquals(new byte[] { 'o', 'k' }, txLog.inProgress.get(uid)._1.toByteArray());

    Assertions.assertArrayEquals(ByteString.copyFromUtf8("test").toByteArray(),
        blobStore.get(Key.of(ByteString.copyFromUtf8("ok"))).get().toNative().toByteArray());

    /// Committing should make the log entry available for streaming
    splitLog.commit(uid._1);

    Assertions.assertEquals(splitLog.from(-1L, Optional.empty()).findFirst(),
        Optional.of(Tuple.of(0L, uid, ByteString.copyFromUtf8("test"))));
  }
}
