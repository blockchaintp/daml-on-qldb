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

import com.blockchaintp.daml.stores.StubStore;
import com.blockchaintp.daml.stores.StubTransactionLog;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;
import com.daml.ledger.participant.state.kvutils.DamlKvutils;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.Offset$;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import io.vavr.API;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

class CoercingTest {
  @Test
  void store_coercion() throws StoreWriteException, StoreReadException {
    var stub = new StubStore<ByteString, ByteString>();

    var coerced = CoercingStore.from(
        Bijection.of((DamlKvutils.DamlStateKey k) -> k.toByteString(),
            API.unchecked((ByteString k) -> DamlKvutils.DamlStateKey.parseFrom(k))),
        Bijection.of((DamlKvutils.DamlStateValue v) -> v.toByteString(),
            API.unchecked((ByteString v) -> DamlKvutils.DamlStateValue.parseFrom(v))),
        stub);

    var k = DamlKvutils.DamlStateKey.newBuilder().setParty("bob").build();
    var v = DamlKvutils.DamlStateValue.newBuilder().build();
    coerced.put(Key.of(k), Value.of(v));

    Assertions.assertArrayEquals(v.toByteArray(), coerced.get(Key.of(k)).get().toNative().toByteArray());

  }

  private static UUID asUuid(final byte[] bytes) {
    var bb = ByteBuffer.wrap(bytes);
    var firstLong = bb.getLong();
    var secondLong = bb.getLong();
    return new UUID(firstLong, secondLong);
  }

  private static byte[] asBytes(final UUID uuid) {
    var bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  @Test
  void txlog_coercion() throws StoreWriteException, StoreReadException {
    var stub = new StubTransactionLog();
    var coerced = CoercingTxLog
        .from(
            Bijection.of((DamlKvutils.DamlLogEntryId k) -> asUuid(k.getEntryId().toByteArray()),
                (UUID k) -> DamlKvutils.DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFrom(asBytes(k)))
                    .build()),
            Bijection.of((DamlKvutils.DamlLogEntry v) -> v.toByteString(),
                API.unchecked((ByteString v) -> DamlKvutils.DamlLogEntry.parseFrom(v))),
            Bijection.of((Offset i) -> Longs.fromByteArray(i.toByteArray()),
                (Long i) -> Offset$.MODULE$.fromByteArray(Longs.toByteArray(i))),
            stub);

    var id = coerced.begin(Optional.empty());
    var data = DamlKvutils.DamlLogEntry.newBuilder().build();
    coerced.sendEvent(id._1, data);
    coerced.commit(id._1);

    var entry = coerced.from(Offset$.MODULE$.fromByteArray(Longs.toByteArray(-1L)), Optional.empty()).findFirst().get();

    Assertions.assertArrayEquals(id._1.toByteArray(), entry._2.toByteArray());

    Assertions.assertArrayEquals(data.toByteArray(), entry._3.toByteArray());
  }

}
