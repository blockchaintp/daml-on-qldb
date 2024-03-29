/*
 * Copyright © 2023 Paravela Limited
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
package com.blockchaintp.daml.stores.s3;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.Value;
import com.blockchaintp.exception.NoSHA512SupportException;
import com.blockchaintp.utility.Aws;
import com.google.protobuf.ByteString;

import io.vavr.API;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * A Store implemented with S3 as the backing store.
 */
public final class S3Store implements Store<ByteString, ByteString> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(S3Store.class);
  private static final int MAX_S3_KEY_LENGTH = 1_024;
  private final String bucketName;
  private final S3AsyncClientBuilder clientBuilder;
  private final UnaryOperator<GetObjectRequest.Builder> getModifications;
  private final UnaryOperator<PutObjectRequest.Builder> putModifications;

  /**
   * Either hex-encode, or hash and prefix if the resulting bucket key will be too long.
   *
   * @param key
   * @return An s3 compatible key.
   */
  private String hashKeyIfNeeded(final ByteString key) {
    if ((key.size() * 2) <= MAX_S3_KEY_LENGTH) {
      return DatatypeConverter.printHexBinary(key.toByteArray());
    }
    try {
      var messageDigest = MessageDigest.getInstance("SHA-512");

      messageDigest.update(key.toByteArray());

      return "hashed/" + DatatypeConverter.printHexBinary(messageDigest.digest());
    } catch (NoSuchAlgorithmException nsae) {
      throw new NoSHA512SupportException(nsae);
    }
  }

  /**
   * Create an S3Store.
   *
   * @param storeName
   *          name of this store, to prevent clashes
   * @param tableName
   *          name of the logical table within this store
   * @param client
   *          the S3 client to use.
   * @param getmods
   *          modifications to S3 storage guarantees
   * @param putmods
   *          modifications to S3 storage guarantees
   */
  public S3Store(final String storeName, final String tableName, final S3AsyncClientBuilder client,
      final UnaryOperator<GetObjectRequest.Builder> getmods, final UnaryOperator<PutObjectRequest.Builder> putmods) {

    this.bucketName = Aws.complyWithS3BucketNaming("vs-" + storeName + "-table-" + tableName);
    this.clientBuilder = client;
    this.getModifications = getmods;
    this.putModifications = putmods;
  }

  /**
   * Return a builder for the provided client.
   *
   * @param client
   *          the client
   * @return a builder based on client
   */
  public static S3StoreBuilder forClient(final S3AsyncClientBuilder client) {
    return new S3StoreBuilder(client);
  }

  private CompletableFuture<Optional<ResponseBytes<GetObjectResponse>>> getObject(final S3AsyncClient client,
      final ByteString key) {
    var get = client.getObject(
        getModifications.apply(GetObjectRequest.builder()).bucket(bucketName).key(hashKeyIfNeeded(key)).build(),
        AsyncResponseTransformer.toBytes());
    return get.exceptionally(e -> {
      if (e.getCause() instanceof NoSuchKeyException) {
        return null;
      } else {
        throw new CompletionException(e.getCause());
      }
    }).thenApply(Optional::ofNullable);
  }

  private <T> T guardRead(final Supplier<T> op) throws StoreReadException {
    try {
      return op.get();
    } catch (CompletionException e) {
      throw new StoreReadException(e.getCause());
    }
  }

  @Override
  public Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    LOG.info("Get {} from bucket {}", key::toNative, () -> bucketName);

    return get(List.of(key)).values().stream().findFirst();
  }

  @Override
  public Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys) throws StoreReadException {
    LOG.info("Get {} items from bucket {}", listOfKeys::size, () -> bucketName);

    var client = clientBuilder.build();
    var futures = listOfKeys.stream()
        .collect(Collectors.<Key<ByteString>, Key<ByteString>, CompletableFuture<Value<ByteString>>>toMap(
            k -> Key.of(k.toNative()), k -> getObject(client, k.toNative())
                .thenApply(x -> x.map(r -> API.unchecked(() -> readResponseBytesAsByteString(r)).get()).orElse(null))));

    var waitOn = new ArrayList<>(futures.values()).toArray(CompletableFuture[]::new);

    return guardRead(() -> {
      CompletableFuture.allOf(waitOn).join();

      return futures.entrySet().stream().filter(v -> v.getValue().join() != null)
          .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().join()));
    });
  }

  private Value<ByteString> readResponseBytesAsByteString(final ResponseBytes<GetObjectResponse> response)
      throws StoreReadException {
    try {
      return Value.of(ByteString.readFrom(response.asInputStream()));
    } catch (IOException e) {
      throw new StoreReadException(e);
    }
  }

  private CompletableFuture<PutObjectResponse> putObject(final S3AsyncClient client, final ByteString key,
      final ByteString blob) {
    return client.putObject(
        putModifications.apply(PutObjectRequest.builder()).bucket(bucketName).key(hashKeyIfNeeded(key)).build(),
        AsyncRequestBody.fromByteBuffer(blob.asReadOnlyByteBuffer()));
  }

  private void guardWrite(final Runnable op) throws StoreWriteException {
    try {
      op.run();
    } catch (CompletionException e) {
      throw new StoreWriteException(e.getCause());
    }
  }

  @Override
  public void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {
    guardWrite(() -> putObject(clientBuilder.build(), key.toNative(), value.toNative()).join());
  }

  @Override
  public void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    var client = clientBuilder.build();
    var futures = listOfPairs.stream().collect(Collectors.toMap(Map.Entry::getKey,
        kv -> putObject(client, kv.getKey().toNative(), kv.getValue().toNative()), (k1, k2) -> k1));

    guardWrite(() -> {
      var waitOn = new ArrayList<>(futures.values()).toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(waitOn).join();

    });
  }
}
