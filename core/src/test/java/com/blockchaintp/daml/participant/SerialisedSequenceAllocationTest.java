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
package com.blockchaintp.daml.participant;

import com.blockchaintp.daml.stores.StubTransactionLog;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.Bijection;
import com.blockchaintp.daml.stores.layers.CoercingTxLog;
import com.blockchaintp.utility.Functions;
import com.blockchaintp.utility.UuidConverter;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.google.protobuf.ByteString;
import io.vavr.API;
import io.vavr.collection.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

class SerialisedSequenceAllocationTest {

  @Test
  void commits_are_ordered_when_highly_concurrent() throws ExecutionException, InterruptedException {
    var stubLog = new StubTransactionLog();
    var writer = CoercingTxLog.from(Bijection.of(UuidConverter::logEntryToUuid, UuidConverter::uuidtoLogEntry),
        Bijection.of(Raw.Envelope::bytes, Raw.Envelope$.MODULE$::apply), Bijection.identity(), stubLog);

    var serialiser = new SerialisedSequenceAllocation(writer);

    var commitIds = new ArrayList<UUID>();
    for (var i = 0; i != 10000; i++) {
      commitIds.add(UUID.randomUUID());
    }

    var executor = Executors.newCachedThreadPool();
    var futures = new ArrayList<CompletableFuture<Long>>();

    commitIds.stream().forEach(key -> {
      var fut = serialiser.serialisedBegin(UuidConverter.uuidtoLogEntry(key))
          .thenCompose(seq -> CompletableFuture.supplyAsync(() -> API.unchecked(() -> {
            writer.sendEvent(UuidConverter.uuidtoLogEntry(key),
                Raw.Envelope$.MODULE$.apply(ByteString.copyFromUtf8("bob")));
            long stopAfter = System.currentTimeMillis() + (long) (Math.random() % 50);
            Awaitility.await().until(new Callable<Boolean>() {
              @Override
              public Boolean call() {
                return System.currentTimeMillis() > stopAfter;
              }
            });
            writer.commit(UuidConverter.uuidtoLogEntry(key));
            return seq;
          }).apply(), executor)).thenCompose(seq -> serialiser.serialisedCommit(seq));
      futures.add(fut);
    });

    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();

    Assertions.assertIterableEquals(
        stubLog.complete.stream().sorted(Comparator.comparing(x -> x._1)).map(x -> x._2).collect(Collectors.toList()),
        commitIds);

  }
}
