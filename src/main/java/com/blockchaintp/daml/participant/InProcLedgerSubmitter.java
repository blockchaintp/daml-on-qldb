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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.address.LedgerAddress;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLogWriter;
import com.blockchaintp.daml.stores.service.Value;
import com.daml.ledger.participant.state.kvutils.DamlKvutils;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.lf.data.Time;
import com.daml.logging.LoggingContext;
import com.google.protobuf.InvalidProtocolBufferException;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.jdk.javaapi.CollectionConverters$;
import scala.jdk.javaapi.OptionConverters$;

/**
 * An in process submitter relying on an ephemeral queue.
 *
 * @param <A>
 * @param <B>
 */
public final class InProcLedgerSubmitter<A extends Identifier, B extends LedgerAddress>
    implements LedgerSubmitter<A, B> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(InProcLedgerSubmitter.class);
  private final KeyValueCommitting committing;
  private final TransactionLogWriter<DamlKvutils.DamlLogEntryId, DamlKvutils.DamlLogEntry, Offset> writer;
  private final Store<DamlKvutils.DamlStateKey, DamlKvutils.DamlStateValue> stateStore;
  private final ExecutionContext context;
  private final String participantId;
  private final Configuration configuration;
  private final LoggingContext loggingContext;
  private final LinkedBlockingQueue<Tuple2<SubmissionReference, CommitPayload<A>>> queue;
  private final ConcurrentHashMap<SubmissionReference, SubmissionStatus> status;

  /**
   * @param theCommitting
   * @param theWriter
   * @param theStateStore
   * @param theContext
   * @param theParticipantId
   * @param theConfiguration
   * @param theLoggingContext
   */
  public InProcLedgerSubmitter(final KeyValueCommitting theCommitting,
      final TransactionLogWriter<DamlKvutils.DamlLogEntryId, DamlKvutils.DamlLogEntry, Offset> theWriter,
      final Store<DamlKvutils.DamlStateKey, DamlKvutils.DamlStateValue> theStateStore,
      final ExecutionContext theContext, final String theParticipantId, final Configuration theConfiguration,
      final LoggingContext theLoggingContext) {
    committing = theCommitting;
    writer = theWriter;
    stateStore = theStateStore;
    context = theContext;
    participantId = theParticipantId;
    configuration = theConfiguration;
    loggingContext = theLoggingContext;
    queue = new LinkedBlockingQueue<>();
    status = new ConcurrentHashMap<>();
    context.execute(this::work);
  }

  private Time.Timestamp getCurrentRecordTime() {
    return Time.Timestamp$.MODULE$.now();
  }

  private <A, B> scala.collection.immutable.Map<A, B> mapToScalaImmutableMap(final java.util.Map<A, B> m) {
    return scala.collection.immutable.Map$.MODULE$.from(CollectionConverters$.MODULE$.asScala(m));
  }

  private <A, B> java.util.Map<A, B> scalaMapToMap(final scala.collection.immutable.Map<A, B> m) {
    return CollectionConverters$.MODULE$.asJava(m);
  }

  /**
   * Do the work of submitting this to our underlying txlog and processing the input and output
   * states.
   */
  private void work() {
    while (true) {
      var next = queue.poll();

      status.put(next._1, SubmissionStatus.PARTIALLY_SUBMITTED);

      var inputKeys = next._2.getReads().stream().map(Identifier::toKey).map(Key::of).collect(Collectors.toList());

      var sparseInputs = inputKeys.stream().collect(Collectors.toMap(k -> k.toNative(),
          k -> OptionConverters$.MODULE$.toScala(Optional.<DamlKvutils.DamlStateValue>empty())));

      try {
        stateStore.get(inputKeys).entrySet().forEach(kv -> sparseInputs.put(kv.getKey().toNative(),
            OptionConverters$.MODULE$.toScala(Optional.of(kv.getValue().toNative()))));

        var entryId = writer.begin();

        var rx = committing.processSubmission(entryId, getCurrentRecordTime(), configuration,
            DamlKvutils.DamlSubmission.parseFrom(next._2.getOperation().getTransaction().getSubmission()),
            participantId, mapToScalaImmutableMap(sparseInputs), loggingContext);

        var outputMap = scalaMapToMap(rx._2);

        stateStore.put(outputMap.entrySet().stream().map(kv -> Map.entry(Key.of(kv.getKey()), Value.of(kv.getValue())))
            .collect(Collectors.toList()));

        writer.sendEvent(entryId, rx._1);
        writer.commit(entryId);

        status.put(next._1, SubmissionStatus.SUBMITTED);

      } catch (StoreWriteException | StoreReadException | InvalidProtocolBufferException e) {
        LOG.error("Could not submit payload {} due to {}", () -> next._1, () -> e);
      }
    }
  }

  @Override
  public SubmissionReference submitPayload(final CommitPayload<A> cp) {
    var ref = new SubmissionReference();
    try {
      queue.put(Tuple.of(ref, cp));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Committer thread has been interrupted!");
      throw new RuntimeException(e);
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
}
