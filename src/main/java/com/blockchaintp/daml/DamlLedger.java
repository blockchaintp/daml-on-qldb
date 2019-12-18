package com.blockchaintp.daml;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.blockchaintp.daml.model.QldbDamlState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;

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
        client.createTable(txn, QldbDamlState.TABLE_NAME);
        client.createTable(txn, QldbDamlLogEntry.TABLE_NAME);
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
}
