package com.blockchaintp.daml;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission;
import com.daml.ledger.participant.state.kvutils.KeyValueSubmission;
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.ledger.participant.state.v1.LedgerInitialConditions;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.ReadService;
import com.daml.ledger.participant.state.v1.SubmissionResult;
import com.daml.ledger.participant.state.v1.SubmitterInfo;
import com.daml.ledger.participant.state.v1.TransactionMeta;
import com.daml.ledger.participant.state.v1.Update;
import com.daml.ledger.participant.state.v1.UploadPackagesResult;
import com.daml.ledger.participant.state.v1.WriteService;
import com.digitalasset.daml.lf.data.Time.Timestamp;
import com.digitalasset.daml.lf.engine.Engine;
import com.digitalasset.daml.lf.transaction.GenTransaction;
import com.digitalasset.daml.lf.value.Value.ContractId;
import com.digitalasset.daml.lf.value.Value.NodeId;
import com.digitalasset.daml.lf.value.Value.VersionedValue;
import com.digitalasset.daml_lf_dev.DamlLf.Archive;
import com.digitalasset.ledger.api.health.HealthStatus;
import com.digitalasset.ledger.api.health.Healthy;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class QldbKvState implements WriteService, ReadService {

  private static final Logger LOG = LoggerFactory.getLogger(QldbKvState.class);
  private String participantId;
  private Engine engine;
  private QldbCommitter committer;
  private String ledgerId;
  private DamlLedger ledger;
  private ScheduledExecutorService executorPool;

  public QldbKvState(String ledgerId, String participantId) {
    this(ledgerId, participantId, new Engine());
  }

  public QldbKvState(String ledgerId, String participantId, Engine engine) {
    this.ledgerId = ledgerId;
    this.engine = engine;
    this.setParticipantId(participantId);
    this.ledger = new DamlLedger(ledgerId);
    this.committer = new QldbCommitter(this.engine, this.ledger, this.getParticipantId());
    this.executorPool = Executors.newScheduledThreadPool(2);
    this.executorPool.submit(this.committer);
  }

  /**
   * @return the participantId
   */
  public String getParticipantId() {
    return participantId;
  }

  /**
   * @param participantId the participantId to set
   */
  public void setParticipantId(String participantId) {
    this.participantId = participantId;
  }

  @Override
  public CompletionStage<UploadPackagesResult> uploadPackages( scala.collection.immutable.List<Archive> payload,
      Option<String> optionalDescription) {

    String sourceDescription = "Uploaded package";
    if (optionalDescription.nonEmpty()) {
      sourceDescription = optionalDescription.get();
    }
    String submissionId = UUID.randomUUID().toString();
    DamlSubmission submission = KeyValueSubmission.archivesToSubmission(submissionId, payload, sourceDescription,
        this.getParticipantId());

    DamlLogEntryId damlLogEntryId = DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(submissionId))
        .build();
    LOG.info("upload package with submissionid {}", submissionId);

    SubmissionResult sr = this.committer.submit(damlLogEntryId, submission);
    if (sr instanceof SubmissionResult.Acknowledged$) {
      return CompletableFuture.completedFuture(new UploadPackagesResult.Ok$());
    } else if (sr instanceof SubmissionResult.Overloaded$) {
      return CompletableFuture.completedFuture(new UploadPackagesResult.Overloaded$());
    } else if (sr instanceof SubmissionResult.NotSupported$) {
      return CompletableFuture.completedFuture(new UploadPackagesResult.NotSupported$());
    } else {
      return CompletableFuture.completedFuture(new UploadPackagesResult.InternalError(sr.description()));
    }
  }

  @Override
  public CompletionStage<SubmissionResult> allocateParty(Option<String> hint, Option<String> displayName, String submissionId) {
    DamlLogEntryId damlLogEntryId = DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(submissionId))
        .build();

    DamlSubmission submission = KeyValueSubmission.partyToSubmission(submissionId, hint, displayName, participantId);
    LOG.info("allocating party with submissionid {}", submissionId);

    return CompletableFuture.completedFuture(this.committer.submit(damlLogEntryId, submission));
  }

  @Override
  public CompletionStage<SubmissionResult> submitConfiguration(Timestamp maxRecordTime, String submissionId,
      Configuration config) {
    DamlLogEntryId damlLogEntryId = DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(submissionId))
        .build();
    DamlSubmission submission = KeyValueSubmission.configurationToSubmission(maxRecordTime, submissionId,
        this.getParticipantId(), config);
    LOG.info("Submitting configuration with submissionid {}", submissionId);

    return CompletableFuture.completedFuture(this.committer.submit(damlLogEntryId, submission));
  }

  private Configuration getDefaultConfiguration() {
    return this.committer.getDefaultConfiguration();
  }

  @Override
  public Source<LedgerInitialConditions, NotUsed> getLedgerInitialConditions() {
    LedgerInitialConditions initialConditions = new LedgerInitialConditions(this.ledgerId, getDefaultConfiguration(),
        getCurrentRecordTime());
    return Source.single(initialConditions);
  }

  private Timestamp getCurrentRecordTime() {
    return this.committer.getCurrentRecordTime();
  }

  @Override
  public Source<Tuple2<Offset, Update>, NotUsed> stateUpdates(Option<Offset> beginAfter) {
    LOG.info("Starting state updates at offset {}", beginAfter);
    long offset = -1L;
    if (!beginAfter.isEmpty()) {
      Offset o = beginAfter.get();
      Collection<Object> oComponents = JavaConverters.asJavaCollection(o.components());
      for (Object oc : oComponents) {
        offset = Long.valueOf(oc.toString());
        break;
      }
    }
    QldbUpdateWatcher watcher = new QldbUpdateWatcher(offset, this.ledger, this.executorPool);
    Thread t = new Thread(watcher,QldbUpdateWatcher.class.getName()+"-from-"+offset);
    t.start();

    return watcher.toSource();
  }


  @Override
  public HealthStatus currentHealth() {
    return Healthy.healthy();
  }

  @Override
  public CompletionStage<SubmissionResult> submitTransaction(SubmitterInfo submitterInfo,
      TransactionMeta meta, GenTransaction<NodeId, ContractId, VersionedValue<ContractId>> tx) {
    String submissionId = UUID.randomUUID().toString();
    LOG.info("Submitting transaction with submissionid {}", submissionId);
    DamlSubmission submission = KeyValueSubmission.transactionToSubmission(submitterInfo, meta, tx);
    DamlLogEntryId damlLogEntryId = DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(submissionId))
        .build();
    return CompletableFuture.completedFuture(this.committer.submit(damlLogEntryId, submission));
 }

}
