package com.blockchaintp.daml;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.blockchaintp.daml.model.QldbDamlState;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.ledger.participant.state.v1.SubmissionResult;
import com.daml.ledger.participant.state.v1.TimeModel;
import com.digitalasset.daml.lf.data.Time.Timestamp;
import com.digitalasset.daml.lf.engine.Engine;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import software.amazon.qldb.QldbSession;

public class QldbCommitter implements Runnable {

  private static Logger LOG = LoggerFactory.getLogger(QldbCommitter.class);
  private Engine engine;
  private DamlLedger ledger;
  private String participantId;

  private LinkedBlockingQueue<Tuple2<DamlLogEntryId, DamlSubmission>> submissionQueue;

  public QldbCommitter(Engine engine, DamlLedger ledger, String participantId) {
    this.engine = engine;
    this.ledger = ledger;
    this.participantId = participantId;
    this.submissionQueue = new LinkedBlockingQueue<>();
  }

  public Configuration getDefaultConfiguration() {
    return new Configuration(0, TimeModel.reasonableDefault());
  }

  public Timestamp getCurrentRecordTime() {
    return new Timestamp(Clock.systemUTC().instant().toEpochMilli() * 1_000_000L);
  }

  public SubmissionResult submit(DamlLogEntryId damlLogEntryId, DamlSubmission submission) {
    try {
      this.submissionQueue.put(Tuple2.apply(damlLogEntryId, submission));
      return new SubmissionResult.Acknowledged$();
    } catch (InterruptedException ioe) {
      Thread.currentThread().interrupt();
      LOG.warn("Committer thread has been interrupted!");
      throw new RuntimeException(ioe);
    }
  }

  public void processSubmission(DamlLogEntryId damlLogEntryId, DamlSubmission submission) {
    QldbSession session = this.ledger.connect();
    java.util.Map<DamlStateKey, Option<DamlStateValue>> inputState = new HashMap<>();
    session.execute(txn -> {
      try {
        for (DamlStateKey k : submission.getInputDamlStateList()) {
          ByteString kbs = KeyValueCommitting.packDamlStateKey(k);
          String skey = kbs.toStringUtf8();
          QldbDamlState s = new QldbDamlState(skey);
          if (s.exists(txn)) {
            s.fetch(txn);
            inputState.put(k, Option.apply(s.damlStateValue()));
          } else {
            inputState.put(k, Option.empty());
          }
        }
      } catch (IOException ioe) {
        LOG.error("IOException fetching from QLDB", ioe);
        txn.abort();
        throw new RuntimeException(ioe);
      }
    }, (retryAttempt) -> {
      LOG.info("Retrying due to OCC conflict");
    });

    Tuple2<DamlLogEntry, Map<DamlStateKey, DamlStateValue>> processedSubmissionScala = KeyValueCommitting
        .processSubmission(this.engine, damlLogEntryId, getCurrentRecordTime(), getDefaultConfiguration(), submission,
            participantId, mapToScalaImmutableMap(inputState));

    DamlLogEntry outputEntry = processedSubmissionScala._1;
    java.util.Map<DamlStateKey, DamlStateValue> outputMap = scalaMapToMap(processedSubmissionScala._2);

    QldbDamlLogEntry newQldbLogEntry = QldbDamlLogEntry.create(damlLogEntryId, outputEntry);
    session.execute(txn -> {
      try {
        newQldbLogEntry.upsert(txn);
        for (java.util.Map.Entry<DamlStateKey, DamlStateValue> mapE : outputMap.entrySet()) {
          QldbDamlState state = QldbDamlState.create(mapE.getKey(), mapE.getValue());
          state.upsert(txn);
        }
      } catch (IOException ioe) {
          LOG.error("IOException fetching from QLDB", ioe);
          txn.abort();
          throw new RuntimeException(ioe);
        }
    }, (retryAttempt) -> {
      LOG.info("Retrying due to OCC conflict");
    });
  }

  @SuppressWarnings("deprecation")
  private <A, B> Map<A, B> mapToScalaImmutableMap(final java.util.Map<A, B> m) {
    return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
  }

  private <A, B> java.util.Map<A, B> scalaMapToMap(final Map<A, B> m) {
    return JavaConverters.mapAsJavaMap(m);
  }

  @Override
  public void run() {

    while (true) {
      try {
        Tuple2<DamlLogEntryId, DamlSubmission> submissionRequest = this.submissionQueue.take();
        this.processSubmission(submissionRequest._1, submissionRequest._2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Committer thread has been interrupted at the main loop");
        throw new RuntimeException(e);
      }
    }
  }
}
