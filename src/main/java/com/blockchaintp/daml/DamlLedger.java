package com.blockchaintp.daml;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;

import com.blockchaintp.daml.exception.NonRecoverableErrorException;
import com.blockchaintp.daml.model.DistributedLedger;
import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.blockchaintp.daml.model.QldbDamlState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;

public final class DamlLedger implements DistributedLedger {

  private static final Logger LOG = LoggerFactory.getLogger(DamlLedger.class);

  private final QLDBServiceClient client;

  private final String ledgerName;

  private AWSCredentialsProvider credentialsProvider;

  private String endpoint;

  private String region;

  private PooledQldbDriver driver;

  private Map<String, byte[]> cache;

  public DamlLedger(final String ledgerName, final int cacheSize) {
    this.client = new QLDBServiceClient();
    this.ledgerName = ledgerName;
    init();

    this.cache = Collections.synchronizedMap(new LRUCache<>(cacheSize));
  }

  public DamlLedger(final String ledgerName) {
    this(ledgerName, 1000);
  }

  private void init() {
    createS3LedgerStore();
    this.driver = createQldbDriver();
    if (!client.ledgerExists(getLedgerName())) {
      LOG.info("Ledger with name: {} does not exist, therefore creating it", getLedgerName());
      client.createLedger(getLedgerName());
      client.waitForActive(getLedgerName());
      QldbSession session = connect();
      session.execute(txn -> {
        client.createTable(txn, QldbDamlState.TABLE_NAME);
        client.createTable(txn, QldbDamlLogEntry.TABLE_NAME);
        client.createTable(txn, "daml_time");
      }, (retryAttempt) -> LOG.info("Retrying due to OCC conflict"));
      session.close();
      client.waitForTable(this.driver, QldbDamlState.TABLE_NAME);
      client.waitForTable(this.driver, QldbDamlLogEntry.TABLE_NAME);
      client.waitForTable(this.driver, "daml_time");
    }
  }

  private void createS3LedgerStore() {
    final S3Client s3 = getS3Client();
    final String bucket = getBucketName();
    if (!bucketExists(bucket)) {
      final CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket)
          .createBucketConfiguration(CreateBucketConfiguration.builder().build()).build();
      s3.createBucket(createBucketRequest);
    }
  }

  private boolean bucketExists(final String bucket) {
    try {
      final S3Client s3 = getS3Client();
      final ListObjectsRequest lbreq = ListObjectsRequest.builder().bucket(bucket).maxKeys(0).build();
      s3.listObjects(lbreq);
      return true;
    } catch (final NoSuchBucketException nsb) {
      return false;
    } catch (final AmazonServiceException ase) {
      if (ase.getErrorCode().equals("NoSuchBucket")) {
        return false;
      } else {
        throw new RuntimeException(
            String.format("S3Bucket named %s exists but this account does not have access to it", bucket));
      }
    }
  }

  private PooledQldbDriver createQldbDriver() {
    final AmazonQLDBSessionClientBuilder builder = AmazonQLDBSessionClientBuilder.standard();
    if (null != endpoint && null != region) {
      builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    }
    if (null != credentialsProvider) {
      builder.setCredentials(credentialsProvider);
    }
    final PooledQldbDriver driver = PooledQldbDriver.builder().withLedger(getLedgerName())
        .withRetryLimit(Constants.RETRY_LIMIT).withSessionClientBuilder(builder).build();
    return driver;
  }

  private S3Client getS3Client() {
    return S3Client.builder().build();
  }

  synchronized public QldbSession connect() {
    return driver.getSession();
  }

  public String getLedgerName() {
    return this.ledgerName;
  }

  public String getBucketName() {
    return "valuestore-" + getLedgerName();
  }

  @Override
  public void putObject(final String key, final byte[] buffer) {
    final S3Client s3 = getS3Client();
    final PutObjectRequest poreq = PutObjectRequest.builder().bucket(getBucketName()).key(key)
        .storageClass(StorageClass.REDUCED_REDUNDANCY).build();
    s3.putObject(poreq, RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer)));
  }

  @Override
  public byte[] getObject(final String key) {
    LOG.info("Fetching {} from bucket {}", key, getBucketName());
    final S3Client s3 = getS3Client();
    final GetObjectRequest getreq = GetObjectRequest.builder().bucket(getBucketName()).key(key).build();
    // S3 can erroneously report that a bucket doesn't exist when it does, so retry a few times
    int attempts = 0;
    for (; attempts < 3; attempts++) {
      try {
        return s3.getObjectAsBytes(getreq).asByteArray();
      } catch (final NoSuchBucketException nsbe) {
        LOG.warn("Failed to find our bucket {}, retrying ...", getBucketName());
      }
    }
    throw new NonRecoverableErrorException(
        String.format("%s: Bucket %s not found after %d attempts - was it deleted?",
            NoSuchBucketException.class.getName(), attempts));
  }

  @Override
  public byte[] getObject(final String key, boolean cacheable) {
    if (!cacheable) {
      return getObject(key);
    }
    if (cache.containsKey(key)) {
      byte[] data = cache.get(key);
      LOG.info("Cache hit for s3key={} size={}", key, data.length);
      return data;
    }
    byte[] data = getObject(key);
    LOG.info("Cache put for s3key={} size={}", key, data.length);
    cache.put(key, data);
    return data;
  }

  @Override
  public boolean existsObject(String key) {
    if (!cache.containsKey(key)) {
      try {
        getObject(key);
      } catch (NoSuchKeyException nske) {
        return false;
      }
    }
    return true;
  }

  private class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 1L;
    private final int cacheSize;

    public LRUCache(int cacheSize) {
      super(16, 0.75f, true);
      this.cacheSize = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() >= cacheSize;
    }
  }
}
