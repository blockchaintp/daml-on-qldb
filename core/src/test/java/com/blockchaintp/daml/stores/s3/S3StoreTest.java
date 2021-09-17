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
package com.blockchaintp.daml.stores.s3;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3StoreTest {

  private static final int CALL_TIMEOUT_MS = 1000;

  @Test
  @SuppressWarnings("unchecked")
  void get_operations_compose_s3_errors_in_store_errors() throws StoreReadException {
    var s3 = mock(S3AsyncClient.class);
    when(s3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(CompletableFuture.supplyAsync(() -> {
          throw ApiCallTimeoutException.create(CALL_TIMEOUT_MS);
        }));

    var builder = mock(S3AsyncClientBuilder.class);

    when(builder.build()).thenReturn(s3);

    var store = new S3Store("", "", builder, x -> x, x -> x);

    var ex = Assertions.assertThrows(StoreReadException.class,
        () -> store.get(Collections.singletonList(Key.of(ByteString.copyFromUtf8("")))));

    Assertions.assertInstanceOf(ApiCallTimeoutException.class, ex.getCause());
  }

  @Test
  void pet_operations_compose_s3_errors_in_store_errors() throws StoreReadException {
    var s3 = mock(S3AsyncClient.class);
    when(s3.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.supplyAsync(() -> {
          throw ApiCallTimeoutException.create(CALL_TIMEOUT_MS);
        }));

    var builder = mock(S3AsyncClientBuilder.class);

    when(builder.build()).thenReturn(s3);

    var store = new S3Store("", "", builder, x -> x, x -> x);

    var ex = Assertions.assertThrows(StoreWriteException.class, () -> store.put(Collections
        .singletonList(Map.entry(Key.of(ByteString.copyFromUtf8("")), Value.of(ByteString.copyFromUtf8(""))))));

    Assertions.assertInstanceOf(ApiCallTimeoutException.class, ex.getCause());
  }
}
