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
import java.util.stream.Stream;

public class StubTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private long offset = 0L;
  public final Map<UUID, ByteString> inprogress = new HashMap<>();
  public final List<Tuple3<Long, UUID, ByteString>> complete = new ArrayList<>();
  public final List<Tuple2<UUID, ByteString>> aborted = new ArrayList<>();

  @Override
  public Stream<Tuple3<Long, UUID, ByteString>> from(Long startInclusive, final Optional<Long> endInclusive)
      throws StoreReadException {
    return complete.stream();
  }

  @Override
  public Optional<Long> getLatestOffset() {
    return Optional.of(offset);
  }

  @Override
  public UUID begin(final Optional<UUID> id) throws StoreWriteException {
    var uuid = id.orElseGet(() -> UUID.randomUUID());
    inprogress.put(uuid, null);

    return uuid;
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    inprogress.put(id, data);
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    complete.add(Tuple.of(offset, txId, inprogress.remove(txId)));
    var ret = offset;
    offset = offset + 1L;

    return offset;
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    aborted.add(Tuple.of(txId, inprogress.remove(txId)));
  }

}
