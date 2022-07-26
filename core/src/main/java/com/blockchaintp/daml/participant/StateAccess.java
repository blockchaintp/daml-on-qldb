/*
 * Copyright 2021-2022 Blockchain Technology Partners
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

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Opaque;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;
import com.blockchaintp.daml.stores.service.Value;
import com.blockchaintp.utility.Functions;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.validator.BatchingLedgerStateOperations;
import com.daml.ledger.validator.LedgerStateAccess;
import com.daml.ledger.validator.LedgerStateOperations;
import com.daml.logging.LoggingContext;

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.immutable.Seq;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.jdk.javaapi.CollectionConverters$;
import scala.jdk.javaapi.FutureConverters;
import scala.jdk.javaapi.OptionConverters$;
import scala.jdk.javaapi.StreamConverters;
import scala.jdk.javaapi.StreamConverters$;
import scala.runtime.BoxedUnit;
import scala.util.Try;

/**
 * Implementation of LedgerStateAccess in terms of abstract state store and a transaction writer.
 */
class StateAccess implements LedgerStateAccess<Long> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(StateAccess.class);
  private final Store<Raw.StateKey, Raw.Envelope> stateStore;
  private final SerialisedSequenceAllocation sequenceAllocation;
  private final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> writer;

  StateAccess(final Store<Raw.StateKey, Raw.Envelope> theStateStore,
      final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> theWriter) {
    stateStore = theStateStore;
    sequenceAllocation = new SerialisedSequenceAllocation(theWriter);
    writer = theWriter;
  }

  private class Operations extends BatchingLedgerStateOperations<Long> {

    @Override
    public Future<Seq<Option<Raw.Envelope>>> readState(final Iterable<Raw.StateKey> keys,
        final ExecutionContext executionContext, final LoggingContext loggingContext) {

      var syncFuture = Future.fromTry(Try.apply(Functions.uncheckFn(() -> {
        var sparseInputs = StreamConverters$.MODULE$.asJavaParStream(keys)
            .collect(Collectors.toMap(k -> k, k -> OptionConverters$.MODULE$.toScala(Optional.<Raw.Envelope>empty())));

        var rx = stateStore.get(sparseInputs.keySet().stream().map(Key::of).collect(Collectors.toList()));

        rx.entrySet().forEach(kv -> sparseInputs.put(kv.getKey().toNative(),
            OptionConverters$.MODULE$.toScala(Optional.of(kv.getValue().toNative()))));

        return CollectionConverters$.MODULE$.asScala(StreamConverters$.MODULE$.asJavaParStream(keys)
            .map(k -> OptionConverters$.MODULE$.toScala(Optional.ofNullable(rx.get(Key.of(k))).map(Opaque::toNative)))
            .collect(Collectors.toList())).toSeq();
      })));

      return Future.delegate(() -> syncFuture, executionContext);

    }

    @Override
    public Future<BoxedUnit> writeState(final Iterable<scala.Tuple2<Raw.StateKey, Raw.Envelope>> keyValuePairs,
        final ExecutionContext executionContext, final LoggingContext loggingContext) {
      LOG.debug("Write state {}", keyValuePairs);
      var syncFuture = Future.fromTry(Try.apply(Functions.uncheckFn(() -> {
        stateStore.put(new ArrayList<>(StreamConverters.asJavaParStream(keyValuePairs)
            .collect(Collectors.toMap(kv -> Key.of(kv._1), kv -> Value.of(kv._2))).entrySet()));
        return BoxedUnit.UNIT;
      })));

      return Future.delegate(() -> syncFuture, executionContext);
    }

    /**
     * Begin allocates sequence, so must sucessfully complete.
     *
     * @param key
     * @param value
     * @param executionContext
     * @return
     */
    @Override
    public Future<Long> appendToLog(final Raw.LogEntryId key, final Raw.Envelope value,
        final ExecutionContext executionContext, final LoggingContext loggingContext) {

      var work = sequenceAllocation.serialisedBegin(key)
          .thenCompose(seq -> CompletableFuture.supplyAsync(() -> Functions.uncheckFn(() -> {
            writer.sendEvent(key, value);
            writer.commit(key);
            return seq;
          }).apply())).thenCompose(sequenceAllocation::serialisedCommit);

      return Future.delegate(() -> FutureConverters.asScala(work), executionContext);
    }

  }

  @Override
  public <T> Future<T> inTransaction(final Function1<LedgerStateOperations<Long>, Future<T>> body,
      final ExecutionContext executionContext, final LoggingContext loggingContext) {
    return Future.delegate(() -> body.apply(new Operations()), executionContext);
  }
}
