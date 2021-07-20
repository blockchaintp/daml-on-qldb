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

import java.util.function.UnaryOperator;

import com.blockchaintp.daml.stores.layers.Retrying;
import com.blockchaintp.daml.stores.service.Store;

import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * A fluent style Builder of S3Store objects.
 */
public class S3StoreBuilder {

  private final S3AsyncClientBuilder client;
  private String storeName;
  private UnaryOperator<PutObjectRequest.Builder> putModifications = x -> x;
  private UnaryOperator<GetObjectRequest.Builder> getModifications = x -> x;
  private String tableName;
  private Retrying.Config retryingConfig;

  /**
   * Construct the builder with the provided s3Client.
   *
   * @param s3Client
   *          the S3 client
   */
  public S3StoreBuilder(final S3AsyncClientBuilder s3Client) {
    this.client = s3Client;
  }

  /**
   * Use the provided name as the store name.
   *
   * @param name
   *          the name of the store
   * @return the builder
   */
  public final S3StoreBuilder forStore(final String name) {
    this.storeName = name;
    return this;
  }

  /**
   * Use the provided name as the table name or sub-grouping of the store.
   *
   * @param name
   *          the name of the table
   * @return the builder
   */
  public final S3StoreBuilder forTable(final String name) {
    this.tableName = name;
    return this;
  }

  /**
   * Use the specified S3 guarantees on put.
   *
   * @param mods
   *          a function which modifies the request properties10
   * @return the builder
   */
  public final S3StoreBuilder onPut(final UnaryOperator<PutObjectRequest.Builder> mods) {
    this.putModifications = mods;

    return this;
  }

  /**
   * Use the specified S3 guarantees on get.
   *
   * @param mods
   *          a function which modifies the request properties10
   * @return the builder
   */
  public final S3StoreBuilder onGet(final UnaryOperator<GetObjectRequest.Builder> mods) {
    this.getModifications = mods;

    return this;
  }

  /**
   * Specify the number of retries to use for the stores built.
   *
   * @param maxRetries
   *          the maximum number of retries.
   * @return the builder
   */
  public final S3StoreBuilder retrying(final int maxRetries) {
    this.retryingConfig = new Retrying.Config();
    this.retryingConfig.setMaxRetries(maxRetries);

    return this;
  }

  /**
   * Build the store and return it.
   *
   * @return the S3Store
   * @throws S3StoreBuilderException
   *           an error in construction
   */
  public final Store<String, byte[]> build() throws S3StoreBuilderException {
    if (this.storeName == null) {
      throw new S3StoreBuilderException("Ledger name must be specified");
    }

    if (this.tableName == null) {
      throw new S3StoreBuilderException("Table name must be specified");
    }

    var store = new S3Store(this.storeName, this.tableName, this.client, this.getModifications, this.putModifications);

    if (retryingConfig != null) {
      return new Retrying<>(retryingConfig, store);
    }

    return store;
  }
}
