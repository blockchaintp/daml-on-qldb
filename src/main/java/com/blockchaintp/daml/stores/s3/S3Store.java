package com.blockchaintp.daml.stores.s3;

import com.blockchaintp.daml.serviceinterface.BlobStore;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class S3Store implements BlobStore<String, byte[]> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(S3Store.class);
  private final String bucketName;
  private final S3AsyncClientBuilder clientBuilder;
  private final UnaryOperator<GetObjectRequest.Builder> getModifications;
  private final UnaryOperator<PutObjectRequest.Builder> putModifications;

  public S3Store(String ledgerName,
                 String tableName,
                 S3AsyncClientBuilder client,
                 UnaryOperator<GetObjectRequest.Builder> getModifications,
                 UnaryOperator<PutObjectRequest.Builder> putModifications) {

    this.bucketName = "vs-" + ledgerName + "-table-" + tableName;
    this.clientBuilder = client;
    this.getModifications = getModifications;
    this.putModifications = putModifications;
  }

  public static S3StoreBuilder forClient(S3AsyncClientBuilder client) {
    return new S3StoreBuilder(client);
  }

  private CompletableFuture<Optional<ResponseBytes<GetObjectResponse>>> getObject(S3AsyncClient client, String key) {
    var get = client.getObject(getModifications.apply(GetObjectRequest
      .builder())
      .bucket(bucketName)
      .key(key)
      .build(), AsyncResponseTransformer.toBytes());
    return get.exceptionally(e -> {
      if (e.getCause() instanceof NoSuchKeyException) {
        return null;
      } else {
        throw new CompletionException(new StoreReadException(e));
      }
    }).thenApply(Optional::ofNullable);
  }

  @Override
  public Optional<Value<byte[]>> get(Key<String> key) throws StoreReadException {
    LOG.info("Get {} from bucket {}", key::toNative, () -> bucketName);

    var response = getObject(clientBuilder.build(), key.toNative())
      .join();

    return response
      .map(x -> new Value<>(x.asByteArray()));
  }

  @Override
  public Map<Key<String>, Value<byte[]>> get(List<Key<String>> listOfKeys) {
    var client = clientBuilder.build();
    var futures = listOfKeys.stream()
      .collect(Collectors.toMap(
        k -> new Key<>(k.toNative()),
        k -> getObject(client, k.toNative())
          .thenApply(x -> x.map(getObjectResponseResponseBytes -> new Value<>(getObjectResponseResponseBytes.asByteArray())).orElse(null))
      ));

    var waitOn = new ArrayList<>(futures
      .values())
      .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(waitOn).join();

    return futures
      .entrySet()
      .stream()
      .filter(v -> v.getValue().join() != null)
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        v -> v.getValue().join()
      ));
  }

  protected CompletableFuture<PutObjectResponse> putObject(S3AsyncClient client, String key, byte[] blob) {
    return client.putObject(putModifications.apply(PutObjectRequest
      .builder())
      .bucket(bucketName)
      .key(key)
      .build(), AsyncRequestBody.fromBytes(blob));
  }

  @Override
  public void put(Key<String> key, Value<byte[]> value) throws StoreWriteException {
    putObject(clientBuilder.build(), key.toNative(), value.toNative()).join();
  }

  @Override
  public void put(List<Map.Entry<Key<String>, Value<byte[]>>> listOfPairs) throws StoreWriteException {
    var client = clientBuilder.build();
    var futures = listOfPairs.stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        kv -> putObject(client,
          kv.getKey().toNative(),
          kv.getValue().toNative()))
      );

    var waitOn = new ArrayList<>(futures.values())
      .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(waitOn).join();
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }
}
