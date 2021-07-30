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

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class StubTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  public final Map<UUID, ByteString> inprogress = new HashMap<>();
  public final List<Map.Entry<UUID, ByteString>> complete = new ArrayList<>();
  public final List<Map.Entry<UUID, ByteString>> aborted = new ArrayList<>();

  @Override
  public Observable<Map.Entry<UUID, ByteString>> from(final Optional<Long> offset) {
    return Observable.fromIterable(complete);
  }

  @Override
  public UUID begin() throws StoreWriteException {
    var uuid = UUID.randomUUID();
    inprogress.put(uuid, null);

    return uuid;
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    inprogress.put(id, data);
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    complete.add(Map.entry(txId, inprogress.remove(txId)));
    return Long.valueOf(complete.size() - 1);
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    aborted.add(Map.entry(txId, inprogress.remove(txId)));
  }

}
