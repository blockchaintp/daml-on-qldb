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
package com.blockchaintp.daml.stores.layers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.exception.NoSHA512SupportException;
import com.google.protobuf.ByteString;

/**
 * A builder of a {@link SplitStore}.
 */
public class SplitStoreBuilder {
  private final Store<ByteString, ByteString> refStore;
  private final Store<ByteString, ByteString> blobs;
  private StoreReader<ByteString, ByteString> reader;
  private UnaryOperator<byte[]> hashFn;
  private boolean writeS3Index = false;
  private LRUCache<ByteString, ByteString> cache;

  /**
   * Create a SplitStoreBuilder.
   *
   * @param refstore
   *          the reference store to use
   * @param blobStore
   */
  public SplitStoreBuilder(final Store<ByteString, ByteString> refstore,
      final Store<ByteString, ByteString> blobStore) {
    this.refStore = refstore;
    this.blobs = blobStore;
    this.hashFn = bytes -> {
      try {
        var messageDigest = MessageDigest.getInstance("SHA-512");

        messageDigest.update(bytes);

        return messageDigest.digest();
      } catch (NoSuchAlgorithmException nsae) {
        throw new NoSHA512SupportException(nsae);
      }
    };
    this.reader = new VerifiedReader(refStore, blobStore);
  }

  /**
   * Whether or not to allow verified reads.
   *
   * @param verified
   *          {@code true} to allow verified reads
   * @return the builder
   */
  public final SplitStoreBuilder verified(final boolean verified) {
    if (verified) {
      this.reader = new VerifiedReader(refStore, blobs);
    } else {
      this.reader = new UnVerifiedReader(blobs, hashFn);
    }

    return this;
  }

  /**
   * Whether to write the S3 index.
   *
   * @param s3Index
   *          {@code true} to write the S3 index
   * @return the builder
   */
  public final SplitStoreBuilder withS3Index(final boolean s3Index) {
    this.writeS3Index = s3Index;

    return this;
  }

  /**
   * Use the given hash function to hash the contents of a blob.
   *
   * @param hasherFn
   *          the hash function
   * @return the builder
   */
  public final SplitStoreBuilder withHasher(final UnaryOperator<byte[]> hasherFn) {
    this.hashFn = hasherFn;

    return this;
  }

  /**
   * Cache the given number of items in memory.
   *
   * @param items
   * @return A configured builder.
   */
  public final SplitStoreBuilder withCaching(final int items) {
    this.cache = new LRUCache<>(items);

    return this;
  }

  /**
   * Build the split store.
   *
   * @return The split store
   */
  public final Store<ByteString, ByteString> build() {
    var inner = new SplitStore(writeS3Index, reader, refStore, blobs, hashFn);

    if (cache != null) {
      return new CachingStore<>(cache, inner);
    }

    return inner;
  }
}
