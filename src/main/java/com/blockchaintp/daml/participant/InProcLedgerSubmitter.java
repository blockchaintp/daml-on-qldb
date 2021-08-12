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

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.layers.CoercingStore;
import com.blockchaintp.daml.stores.layers.CoercingTxLog;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;
import com.blockchaintp.daml.stores.service.Value;
import com.blockchaintp.utility.UuidConverter;
import com.daml.caching.Cache;
import com.daml.ledger.participant.state.kvutils.DamlKvutils;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.ledger.validator.LedgerStateAccess;
import com.daml.ledger.validator.LedgerStateOperations;
import com.daml.ledger.validator.SubmissionValidator;
import com.daml.ledger.validator.ValidatingCommitter;
import com.daml.lf.data.Time;
import com.daml.lf.engine.Engine;
import com.daml.logging.LoggingContext;
import com.daml.metrics.Metrics;
import com.google.protobuf.ByteString;

import io.vavr.CheckedFunction0;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.immutable.Seq;
import scala.concurrent.Await$;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.jdk.javaapi.CollectionConverters$;
import scala.jdk.javaapi.OptionConverters$;
import scala.jdk.javaapi.StreamConverters;
import scala.jdk.javaapi.StreamConverters$;
import scala.runtime.BoxedUnit;
import scala.util.Try;

/**
 * An in process submitter relying on an ephemeral queue.
 *
 * @param <A>
 * @param <B>
 */
public final class InProcLedgerSubmitter<A extends Identifier, B extends LedgerAddress>
    implements LedgerSubmitter<A, B> {

  private final ValidatingCommitter<Long> comitter;
  private final Engine engine;
  private final Metrics metrics;

  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(InProcLedgerSubmitter.class);
  private final TransactionLogWriter<Raw.LogEntryId, Raw.Envelope, Long> writer;
  private final Store<Raw.StateKey, Raw.Envelope> stateStore;
  private final String participantId;
  private final Configuration configuration;
  private final LoggingContext loggingContext;
  private final LinkedBlockingQueue<Tuple2<SubmissionReference, CommitPayload<A>>> queue;
  private final ConcurrentHashMap<SubmissionReference, SubmissionStatus> status;
  private final ExecutionContext context;

  private class StateAccess implements LedgerStateAccess<Long> {

    private class Operations implements LedgerStateOperations<Long> {

      @Override
      public Future<Option<Raw.Envelope>> readState(final Raw.StateKey key, final ExecutionContext executionContext) {
        return Future.fromTry(Try.apply(
            uncheckFn(() -> OptionConverters$.MODULE$.toScala(stateStore.get(Key.of(key)).map(v -> v.toNative())))));
      }

      @Override
      public Future<Seq<Option<Raw.Envelope>>> readState(final Iterable<Raw.StateKey> keys,
          final ExecutionContext executionContext) {
        return Future.fromTry(Try.apply(uncheckFn(() -> {
          var sparseInputs = StreamConverters$.MODULE$.asJavaParStream(keys).collect(
              Collectors.toMap(k -> k, k -> OptionConverters$.MODULE$.toScala(Optional.<Raw.Envelope>empty())));

          var rx = stateStore.get(sparseInputs.keySet().stream().map(Key::of).collect(Collectors.toList()));

          rx.entrySet().forEach(kv -> sparseInputs.put(kv.getKey().toNative(),
              OptionConverters$.MODULE$.toScala(Optional.of(kv.getValue().toNative()))));

          return CollectionConverters$.MODULE$.asScala(StreamConverters$.MODULE$.asJavaParStream(keys)
              .map(k -> OptionConverters$.MODULE$.toScala(Optional.of(rx.get(k)).map(v -> v.toNative())))
              .collect(Collectors.toList())).toSeq();
        })));
      }

      @Override
      public Future<BoxedUnit> writeState(final Raw.StateKey key, final Raw.Envelope value,
          final ExecutionContext executionContext) {
        return Future.fromTry(Try.apply(uncheckFn(() -> {
          stateStore.put(Key.of(key), Value.of(value));
          return BoxedUnit.UNIT;
        })));
      }

      @Override
      public Future<BoxedUnit> writeState(final Iterable<scala.Tuple2<Raw.StateKey, Raw.Envelope>> keyValuePairs,
          final ExecutionContext executionContext) {
        return Future.fromTry(Try.apply(uncheckFn(() -> {
          stateStore.put(StreamConverters.asJavaParStream(keyValuePairs)
              .collect(Collectors.toMap(kv -> Key.of(kv._1), kv -> Value.of(kv._2))).entrySet().stream()
              .collect(Collectors.toList()));
          return BoxedUnit.UNIT;
        })));
      }

      @Override
      public Future<Long> appendToLog(final Raw.LogEntryId key, final Raw.Envelope value,
          final ExecutionContext executionContext) {
        return Future.fromTry(Try.apply(uncheckFn(() -> {
          writer.sendEvent(key, value);
          return writer.commit(key);
        })));
      }
    }

    @Override
    public <T> Future<T> inTransaction(final Function1<LedgerStateOperations<Long>, Future<T>> body,
        final ExecutionContext executionContext) {
      return body.apply(new Operations());
    }
  }

  /**
   *
   * @param <I>
   * @param <A>
   * @return An appropriate InProcLedgerSubmitterBuilder.
   */
  public static <I extends Identifier, A extends LedgerAddress> InProcLedgerSubmitterBuilder<I, A> builder() {
    return new InProcLedgerSubmitterBuilder<>();
  }

  /**
   * @param theEngine
   * @param theMetrics
   * @param theTxLog
   * @param theStateStore
   * @param theParticipantId
   * @param theConfiguration
   * @param theLoggingContext
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public InProcLedgerSubmitter(final Engine theEngine, final Metrics theMetrics,
      final TransactionLog<UUID, ByteString, Long> theTxLog, final Store<ByteString, ByteString> theStateStore,
      final String theParticipantId, final Configuration theConfiguration, final LoggingContext theLoggingContext) {
    engine = theEngine;
    metrics = theMetrics;
    writer = CoercingTxLog.writerFrom(
        (UUID k) -> Raw.LogEntryId$.MODULE$.apply(ByteString.copyFrom(UuidConverter.asBytes(k))),
        Raw.Envelope$.MODULE$::apply, (Long i) -> i,
        (Raw.LogEntryId k) -> UuidConverter.asUuid(k.bytes().toByteArray()), Raw.Envelope::bytes, l -> l, theTxLog);
    stateStore = CoercingStore.from(Raw.StateKey$.MODULE$::apply, Raw.Envelope$.MODULE$::apply, Raw.StateKey::bytes,
        Raw.Envelope::bytes, theStateStore);
    participantId = theParticipantId;
    configuration = theConfiguration;
    loggingContext = theLoggingContext;
    queue = new LinkedBlockingQueue<>();
    status = new ConcurrentHashMap<>();

    comitter = new ValidatingCommitter<Long>(Instant::now,
        SubmissionValidator.create(new StateAccess(),
            uncheckFn(() -> DamlKvutils.DamlLogEntryId.newBuilder().setEntryId(writer.begin().bytes()).build()), false,
            Cache.none(), engine, metrics),
        x -> BoxedUnit.UNIT);

    context = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor());

    scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor()).execute(this::work);
  }

  private void work() {
    var run = true;
    while (run) {
      try {
        var next = queue.take();

        status.put(next._1, SubmissionStatus.PARTIALLY_SUBMITTED);

        var res = Await$.MODULE$.result(this.comitter.commit(next._2.getCorrelationId(), next._2.getSubmission(),
            next._2.getSubmittingParticipantId(), context), Duration.Inf());

        LOG.info("Result %s", res.toString());

        status.put(next._1, SubmissionStatus.SUBMITTED);

      } catch (TimeoutException | InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Thread interrupted", e);

        run = false;
      }
    }
  }

  private Time.Timestamp getCurrentRecordTime() {
    return Time.Timestamp$.MODULE$.now();
  }

  private <A1, B1> scala.collection.immutable.Map<A1, B1> mapToScalaImmutableMap(final java.util.Map<A1, B1> m) {
    return scala.collection.immutable.Map$.MODULE$.from(CollectionConverters$.MODULE$.asScala(m));
  }

  private <A1, B1> java.util.Map<A1, B1> scalaMapToMap(final scala.collection.immutable.Map<A1, B1> m) {
    return CollectionConverters$.MODULE$.asJava(m);
  }

  @Override
  public SubmissionReference submitPayload(final CommitPayload<A> cp) {
    var ref = new SubmissionReference();
    try {
      queue.put(Tuple.of(ref, cp));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Committer thread has been interrupted!");
    }
    status.put(ref, SubmissionStatus.ENQUEUED);

    return ref;
  }

  @Override
  public Optional<SubmissionStatus> checkSubmission(final SubmissionReference ref) {
    return Optional.of(status.get(ref));
  }

  @Override
  public CommitPayload<B> translatePayload(final CommitPayload<A> cp) {
    return null;
  }

  private <T> Function0<T> uncheckFn(final CheckedFunction0<T> fn) {
    return () -> fn.unchecked().apply();
  }
}
