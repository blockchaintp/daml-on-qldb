package com.blockchaintp.daml;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.blockchaintp.daml.exception.NonRecoverableErrorException;
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
import software.amazon.qldb.Transaction;

public class QldbCommitter implements Runnable {

  private static Logger LOG = LoggerFactory.getLogger(QldbCommitter.class);

  private final Engine engine;
  private final DamlLedger ledger;
  private final String participantId;

  private LinkedBlockingQueue<SubmissionWrapper> submissionQueue;

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
    return new Timestamp(TimeUnit.SECONDS.toMicros(Clock.systemUTC().instant().getEpochSecond()));
  }

  public SubmissionResult submit(DamlLogEntryId damlLogEntryId, DamlSubmission damlSubmission) {
    SubmissionWrapper submission = new SubmissionWrapper(damlLogEntryId, damlSubmission);
    try {
      this.submissionQueue.put(submission);
      return new SubmissionResult.Acknowledged$();
    } catch (InterruptedException ioe) {
      Thread.currentThread().interrupt();
      LOG.warn("Committer thread has been interrupted!");
      throw new RuntimeException(ioe);
    }
  }

  public void processSubmission(SubmissionWrapper submission) {
    QldbSession session = this.ledger.connect();
    Transaction txn = session.startTransaction();
    java.util.Map<DamlStateKey, Option<DamlStateValue>> inputState = new HashMap<>();
    try {
      List<QldbDamlState> stateRefreshList=new ArrayList<>();
      for (DamlStateKey k : submission.getDamlSubmission().getInputDamlStateList()) {
        ByteString kbs = KeyValueCommitting.packDamlStateKey(k);
        String skey = kbs.toStringUtf8();
        QldbDamlState s = new QldbDamlState(this.ledger, skey);
        if (s.exists(txn)) {
          s.fetch(txn);
          stateRefreshList.add(s);
          inputState.put(k, Option.apply(s.damlStateValue()));
        } else {
          inputState.put(k, Option.empty());
        }
      }
      for (QldbDamlState s: stateRefreshList) {
        s.refreshFromBulkStore();
      }
    } catch (IOException ioe) {
      LOG.error("IOException committing data", ioe);
      throw new RuntimeException(ioe);
    }
    session.close();
    session = null;

    LOG.info("Processing submission for logIdEntry={}", submission.getDamlLogEntryId());
    Tuple2<DamlLogEntry, Map<DamlStateKey, DamlStateValue>> processedSubmissionScala = KeyValueCommitting
        .processSubmission(this.engine, submission.getDamlLogEntryId(), getCurrentRecordTime(),
            getDefaultConfiguration(), submission.getDamlSubmission(), participantId, mapToScalaImmutableMap(inputState));

    DamlLogEntry outputEntry = processedSubmissionScala._1;
    java.util.Map<DamlStateKey, DamlStateValue> outputMap = scalaMapToMap(processedSubmissionScala._2);

    try {
      QldbDamlLogEntry newQldbLogEntry = QldbDamlLogEntry.create(this.ledger, submission.getDamlLogEntryId(), outputEntry);
      List<QldbDamlState> stateList = new ArrayList<>();
      for (java.util.Map.Entry<DamlStateKey, DamlStateValue> mapE : outputMap.entrySet()) {
        QldbDamlState state = QldbDamlState.create(this.ledger, mapE.getKey(), mapE.getValue());
        stateList.add(state);
        state.updateBulkStore();
      }

      newQldbLogEntry.updateBulkStore();
      for (java.util.Map.Entry<DamlStateKey, DamlStateValue> mapE : outputMap.entrySet()) {
        QldbDamlState state = QldbDamlState.create(this.ledger, mapE.getKey(), mapE.getValue());
        stateList.add(state);
      }
      session = this.ledger.connect();
      txn = session.startTransaction();
      newQldbLogEntry.upsert(txn);
      for (QldbDamlState s : stateList) {
        s.upsert(txn);
      }
      txn.commit();
    } catch (Throwable ioe) {
      LOG.error("IOException committing data", ioe);
      throw new RuntimeException(ioe);
    }
    session.close();
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
        SubmissionWrapper submission = this.submissionQueue.take();
        this.processSubmission(submission);
      } catch (NonRecoverableErrorException nree) {
        LOG.error("{}: Cannot recover, shutting down...", nree);
        System.exit(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Committer thread has been interrupted at the main loop");
        throw new RuntimeException(e);
      } catch (Throwable e) {
        LOG.warn("Committer thread has been interrupted at the main loop",e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * DAML log entry a submission pair wrapper.
   */
  private class SubmissionWrapper {
    private final DamlLogEntryId damlLogEntryId;
    private final DamlSubmission damlSubmission;
    
    public SubmissionWrapper(final DamlLogEntryId newDamlLogEntryId, final DamlSubmission newDamlSubmission) {
      this.damlLogEntryId = newDamlLogEntryId;
      this.damlSubmission = newDamlSubmission;
    }
                                           
    public DamlLogEntryId getDamlLogEntryId() {
      return this.damlLogEntryId;
    }

    public DamlSubmission getDamlSubmission() {
      return this.damlSubmission;
    }
  }
}
