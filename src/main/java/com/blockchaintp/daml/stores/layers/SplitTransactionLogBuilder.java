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
package com.blockchaintp.daml.stores.layers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.function.UnaryOperator;

import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.exception.NoSHA512SupportException;
import com.google.protobuf.ByteString;

/**
 *
 */
public class SplitTransactionLogBuilder {
  private UnaryOperator<byte[]> hashFn;
  private TransactionLog<UUID, ByteString, Long> txLog;
  private Store<String, byte[]> blobs;

  /**
   *
   * @param theTxLog
   * @param theBlobs
   */
  public SplitTransactionLogBuilder(final TransactionLog<UUID, ByteString, Long> theTxLog,
      final Store<String, byte[]> theBlobs) {
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
   * @return the builder
   */
  public final SplitTransactionLogBuilder withHasher(final UnaryOperator<byte[]> hasherFn) {
    this.hashFn = hasherFn;

    return this;
  }

  /**
   * Build the split store.
   *
   * @return the split store
   */
  public final TransactionLog<UUID, ByteString, Long> build() {
    return new SplitTransactionLog(txLog, blobs, hashFn);
  }
}
