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
package com.blockchaintp.daml.participant;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
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

import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.immutable.Seq;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;
import scala.jdk.javaapi.CollectionConverters$;
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
  public static final int DELAY = 50;
  private final ConcurrentLinkedDeque<Raw.LogEntryId> beginRx = new ConcurrentLinkedDeque<>();
  private final ConcurrentLinkedDeque<Raw.LogEntryId> beginTx = new ConcurrentLinkedDeque<>();
  private final CommitHighwaterMark highwaterMark = new CommitHighwaterMark();
  private final Store<Raw.StateKey, Raw.Envelope> stateStore;
  private final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> writer;
  private final ExecutionContextExecutor beginExecutor = ExecutionContext
      .fromExecutor(Executors.newSingleThreadExecutor());
  private boolean running = true;

  StateAccess(final Store<Raw.StateKey, Raw.Envelope> theStateStore,
      final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> theWriter) {
    stateStore = theStateStore;
    writer = theWriter;
    beginExecutor.execute(this::beginRunner);
  }

  private void beginRunner() {
    while (running) {
      var next = beginRx.poll();
      if (next != null) {
        LOG.trace("Poll begin rx {}", next);
        var started = false;
        while (!started) {
          try {
            var r = writer.begin(Optional.of(next));
            highwaterMark.begin(r._2);
            LOG.info("Transaction seq {} {} begun, enqueing", r._2, r._1);
            started = true;
          } catch (StoreWriteException e) {
            LOG.error("Failed to start transaction {}, retry", e);
          }
        }
        beginTx.add(next);
      }
      try {
        Thread.sleep(DELAY);
      } catch (InterruptedException theE) {
        running = false;
        Thread.currentThread().interrupt();
      }
    }

    running = false;
  }

  private class Operations extends BatchingLedgerStateOperations<Long> {

    @Override
    public Future<Seq<Option<Raw.Envelope>>> readState(final Iterable<Raw.StateKey> keys,
        final ExecutionContext executionContext) {

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
        final ExecutionContext executionContext) {
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
        final ExecutionContext executionContext) {
      beginRx.add(key);

      var found = false;
      do {
        var head = beginTx.peek();
        found = (head != null && head.equals(key));
        Thread.yield();
      } while (!found);

      LOG.info("Transaction {} started, continuing append", beginTx.peekFirst());
      beginTx.pop();

      return Future.delegate(() -> Future.fromTry(Try.apply(Functions.uncheckFn(() -> {
        writer.sendEvent(key, value);
        var r = writer.commit(key);
        highwaterMark.complete(r);

        do {
          highwaterMark.highestCommitted(r);
          Thread.sleep(DELAY);
        } while (highwaterMark.running(r));

        return r;
      }))), executionContext);
    }

  }

  @Override
  public <T> Future<T> inTransaction(final Function1<LedgerStateOperations<Long>, Future<T>> body,
      final ExecutionContext executionContext) {
    return Future.delegate(() -> body.apply(new Operations()), executionContext);
  }
}
