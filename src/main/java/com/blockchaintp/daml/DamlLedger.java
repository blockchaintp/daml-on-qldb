package com.blockchaintp.daml;

import java.nio.ByteBuffer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.blockchaintp.daml.model.QldbDamlState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
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

  private PooledQldbDriver driver;

  public DamlLedger(final String ledgerName) {
    this.client = new QLDBServiceClient();
    this.ledgerName = ledgerName;
    init();
  }

  public void init() {
    this.driver = createQldbDriver();
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

    createS3LedgerStore();
  }

  private void createS3LedgerStore() {
    S3Client s3 = getS3Client();
    String bucket = getBucketName();
    if (!bucketExists(bucket)) {
      CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).createBucketConfiguration(
        CreateBucketConfiguration.builder().build()
      ).build();
      s3.createBucket(createBucketRequest);
    }
  }

  public String getBucketName() {
    return "valuestore-" + this.ledgerName;
  }

  public S3Client getS3Client() {
    return S3Client.builder().build();
  }

  public void s3UploadValue(String key, ByteBuffer buffer) {
    S3Client s3 = getS3Client();
    PutObjectRequest poreq = PutObjectRequest.builder().bucket(getBucketName()).key(key).build();
    s3.putObject(poreq, RequestBody.fromByteBuffer(buffer));
  }

  private boolean bucketExists(String bucket) {
    try {
      S3Client s3 = S3Client.builder().build();
      ListObjectsRequest lbreq = ListObjectsRequest.builder().bucket(bucket).maxKeys(0).build();
      s3.listObjects(lbreq);
      return true;
    } catch (AmazonServiceException ase) {
      if (ase.getErrorCode().equals("NoSuchBucket")) {
        return false;
      } else {
        throw new RuntimeException(String.format("S3Bucket named %s exists but this account does not have access to it", bucket));
      }
    }
  }

  public PooledQldbDriver createQldbDriver() {
    AmazonQLDBSessionClientBuilder builder = AmazonQLDBSessionClientBuilder.standard();
    if (null != endpoint && null != region) {
      builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    }
    if (null != credentialsProvider) {
      builder.setCredentials(credentialsProvider);
    }
    PooledQldbDriver driver = PooledQldbDriver.builder().withLedger(ledgerName).withRetryLimit(Constants.RETRY_LIMIT)
        .withSessionClientBuilder(builder).build();
    return driver;
  }

  public QldbSession connect() {
    return driver.getSession();
  }
}
