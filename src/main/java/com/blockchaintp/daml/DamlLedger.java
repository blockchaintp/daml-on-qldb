package com.blockchaintp.daml;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlConfiguration;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlConfigurationEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Result;

public final class DamlLedger {

  private static final Logger LOG = LoggerFactory.getLogger(DamlLedger.class);

  private QLDBServiceClient client;

  private QldbSession session;

  private String ledgerName;

  private AWSCredentialsProvider credentialsProvider;

  private String endpoint;

  private String region;

  public DamlLedger(final String ledgerName) {
    this.client = new QLDBServiceClient();
    this.ledgerName = ledgerName;
    init();
  }

  public void init() {
    if (!client.ledgerExists(ledgerName)) {
      LOG.info("Ledger with name: {} does not exist, therefore creating it", ledgerName);
      client.createLedger(ledgerName);
      client.waitForActive(ledgerName);
      connect();
      session.execute(txn -> {
        client.createTable(txn, "daml_state");
        client.createTable(txn, Constants.DAML_LOG_TABLE_NAME);
        client.createTable(txn, "daml_time");
      }, (retryAttempt) -> LOG.info("Retrying due to OCC conflict"));

    }
  }

  public QldbSession connect() {
    synchronized (this) {
      if (session == null) {
        AmazonQLDBSessionClientBuilder builder = AmazonQLDBSessionClientBuilder.standard();
        if (null != endpoint && null != region) {
          builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }
        if (null != credentialsProvider) {
          builder.setCredentials(credentialsProvider);
        }
        PooledQldbDriver driver = PooledQldbDriver.builder().withLedger(ledgerName)
            .withRetryLimit(Constants.RETRY_LIMIT).withSessionClientBuilder(builder).build();

        session = driver.getSession();
      }
    }
    return session;
  }

  public <T> void insertDocuments(final String tableName, final List<T> documents) {
    connect();
    session.execute(txn -> {
      client.insertDocuments(txn, tableName, documents);
    }, (retryAttempt) -> LOG.info("Retrying due to OCC conflict"));
  }

  public static void main(String[] args) {
    DamlLedger ledger = new DamlLedger("test-daml-on-qldb");

    List<QldbDamlLogEntry> list = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      DamlLogEntryId testId = DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8("test-entry-" + i))
          .build();
      DamlLogEntry test = DamlLogEntry.newBuilder().setConfigurationEntry(DamlConfigurationEntry.newBuilder()
          .setConfiguration(DamlConfiguration.newBuilder().setOpenWorld(true).build()).build()).build();
      QldbDamlLogEntry entry = QldbDamlLogEntry.create(testId, test);
      list.add(entry);
    }
    ledger.insertDocuments(Constants.DAML_LOG_TABLE_NAME, list);
  }
}
