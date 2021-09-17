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

import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Opaque;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory;

/**
 * TransactionLog composing a transaction log and an S3 store to keep large values outside of the
 * transaction log.
 */
public final class SplitTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private static final LambdaLogger LOG = LambdaLoggerFactory.getLogger(SplitTransactionLog.class);
  private final TransactionLog<UUID, ByteString, Long> txLog;
  private final Store<ByteString, ByteString> blobs;
  private final UnaryOperator<byte[]> hashFn;

  /**
   * Convenience method for split transaction log builder access.
   *
   * @param theTxLog
   * @param blobs
   * @return A partially configured builder.
   */
  public static SplitTransactionLogBuilder from(final TransactionLog<UUID, ByteString, Long> theTxLog,
      final Store<ByteString, ByteString> blobs) {
    return new SplitTransactionLogBuilder(theTxLog, blobs);
  }

  /**
   * Create a split transaction log.
   *
   * @param txlog
   * @param blobStore
   * @param hasher
   */
  public SplitTransactionLog(final TransactionLog<UUID, ByteString, Long> txlog,
      final Store<ByteString, ByteString> blobStore, final UnaryOperator<byte[]> hasher) {
    this.txLog = txlog;
    this.blobs = blobStore;
    this.hashFn = hasher;
  }

  /**
   * Reads through txlog keys then enriches with blob data, inefficient without some sort of sliding
   * window and buffering.
   *
   * @param startExclusive
   * @param endInclusive
   * @return A stream of data combined from blob and ledger.
   * @throws StoreReadException
   */
  @Override
  public Stream<Tuple3<Long, UUID, ByteString>> from(final Long startExclusive,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Long> endInclusive)
      throws StoreReadException {

    return txLog
        .from(startExclusive,
            endInclusive)
        .map(
            r -> API
                .unchecked(() -> Tuple.of(r._1, r._2,
                    blobs.get(Key.of(r._3)).map(Opaque::toNative)
                        .orElseThrow(() -> new StoreReadException(SpltStoreException.missingData()))))
                .apply())
        .map(x -> {
          LOG.debug("Yield log entry {} {}", x._1, x._2);
          return x;
        });
  }

  @Override
  public Optional<Long> getLatestOffset() {
    return txLog.getLatestOffset();
  }

  @Override
  public UUID begin(final Optional<UUID> id) throws StoreWriteException {
    return txLog.begin(id);
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    LOG.debug("Send event {} {}", id, data);
    var hash = hashFn.apply(data.toByteArray());

    blobs.put(Key.of(ByteString.copyFrom(hash)), Value.of(data));
    txLog.sendEvent(id, ByteString.copyFrom(hash));
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    LOG.debug("Commit {}", txId);
    return txLog.commit(txId);
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    txLog.abort(txId);
  }
}
