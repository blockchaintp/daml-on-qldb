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
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.exception.NoSHA512SupportException;
import com.google.protobuf.ByteString;

/**
 *
 */
public class SplitTransactionLogBuilder {
  private UnaryOperator<byte[]> hashFn;
  private final TransactionLog<UUID, ByteString, Long> txLog;
  private final Store<ByteString, ByteString> blobs;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<Integer> cacheSize = Optional.empty();
  private Optional<Integer> retries;

  /**
   * @param theTxLog
   * @param theBlobs
   */
  public SplitTransactionLogBuilder(final TransactionLog<UUID, ByteString, Long> theTxLog,
      final Store<ByteString, ByteString> theBlobs) {
    txLog = theTxLog;
    blobs = theBlobs;
    this.hashFn = bytes -> {
      try {
        var messageDigest = MessageDigest.getInstance("SHA-512");

        messageDigest.update(bytes);

        return messageDigest.digest();
      } catch (NoSuchAlgorithmException nsae) {
        throw new NoSHA512SupportException(nsae);
      }
    };
  }

  /**
   * Use the given hash function to hash the contents of a blob.
   *
   * @param hasherFn
   *          the hash function
   * @return A configured builder
   */
  public final SplitTransactionLogBuilder withHasher(final UnaryOperator<byte[]> hasherFn) {
    this.hashFn = hasherFn;

    return this;
  }

  /**
   * Cache the last n comitted or read transactions.
   *
   * @param theCacheSize
   * @return A configured builder.
   */
  public final SplitTransactionLogBuilder withCache(final int theCacheSize) {
    this.cacheSize = Optional.of(theCacheSize);

    return this;
  }

  /**
   * Retry txlog operations this many times.
   *
   * @param theRetries
   * @return A configured builder.
   */
  public final SplitTransactionLogBuilder withRetries(final int theRetries) {
    this.retries = Optional.of(theRetries);

    return this;
  }

  /**
   * Build the split store.
   *
   * @return the split store
   */
  public final TransactionLog<UUID, ByteString, Long> build() {

    TransactionLog<UUID, ByteString, Long> split = new SplitTransactionLog(txLog, blobs, hashFn);

    if (cacheSize.isPresent()) {
      split = new CachingTransactionLog<>(new LRUCache<>(cacheSize.get()), new LRUCache<>(cacheSize.get()), split,
          (s, e) -> StreamSupport.stream(LongStream.range(s, e.orElse(s + 1)).spliterator(), false));
    }

    if (retries.isPresent()) {
      split = new RetryingTransactionLog<>(retries.get(), split);
    }

    return split;
  }
}
