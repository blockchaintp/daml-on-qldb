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
package com.blockchaintp.daml.stores;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.google.protobuf.ByteString;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class StubTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private long offset = 0L;
  public final Map<UUID, Tuple2<ByteString, Long>> inProgress = new HashMap<>();
  public final List<Tuple3<Long, UUID, ByteString>> complete = new ArrayList<>();
  public final List<Tuple2<UUID, ByteString>> aborted = new ArrayList<>();

  @Override
  public Stream<Tuple3<Long, UUID, ByteString>> from(Long startInclusive, final Optional<Long> endInclusive)
      throws StoreReadException {
    synchronized (this) {
      return complete.stream();
    }
  }

  @Override
  public Optional<Long> getLatestOffset() {
    return Optional.of(offset);
  }

  @Override
  public Tuple2<UUID, Long> begin(final Optional<UUID> id) throws StoreWriteException {
    synchronized (this) {
      var uuid = id.orElseGet(() -> UUID.randomUUID());
      inProgress.put(uuid, Tuple.of(null, offset = offset + 1L));

      return Tuple.of(uuid, offset);
    }
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    synchronized (this) {
      inProgress.put(id, Tuple.of(data, inProgress.get(id)._2));
    }
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    synchronized (this) {
      var cur = inProgress.remove(txId);
      complete.add(Tuple.of(cur._2, txId, cur._1));

      return cur._2;
    }
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    synchronized (this) {
      var cur = inProgress.remove(txId);
      aborted.add(Tuple.of(txId, cur._1));
    }
  }

}
