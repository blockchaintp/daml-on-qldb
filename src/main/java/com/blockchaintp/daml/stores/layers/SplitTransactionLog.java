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

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import io.vavr.Tuple;
import io.vavr.Tuple3;

/**
 * TransactionLog composing a transaction log and an S3 store to keep large values outside of the
 * transaction log.
 */
public final class SplitTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private final TransactionLog<UUID, ByteString, Long> txLog;
  private final Store<String, byte[]> blobs;
  private final UnaryOperator<byte[]> hashFn;

  /**
   * Convenience method for split transaction log builder access.
   *
   * @param theTxLog
   * @param blobs
   * @return A partially configured builder.
   */
  public static SplitTransactionLogBuilder from(final TransactionLog<UUID, ByteString, Long> theTxLog,
      final Store<String, byte[]> blobs) {
    return new SplitTransactionLogBuilder(theTxLog, blobs);
  }

  /**
   * Create a split transaction log.
   *
   * @param txlog
   * @param blobStore
   * @param hasher
   */
  public SplitTransactionLog(final TransactionLog<UUID, ByteString, Long> txlog, final Store<String, byte[]> blobStore,
      final UnaryOperator<byte[]> hasher) {
    this.txLog = txlog;
    this.blobs = blobStore;
    this.hashFn = hasher;
  }

  /**
   * Reads through txlog keys then enriches with blob data, using bulk fetch, this may cause memory
   * issues with large data sets and should possibly be paged.
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

    var resultsByS3Key = txLog.from(startExclusive, endInclusive)
        .collect(Collectors.toMap(r -> Key.of(DatatypeConverter.printHexBinary(r._3.toByteArray())), r -> r));

    var blobData = blobs.get(new ArrayList<>(resultsByS3Key.keySet()));

    var diff = Sets.difference(resultsByS3Key.keySet(), blobData.keySet());

    if (!diff.isEmpty()) {
      throw new StoreReadException(SpltStoreException.missingS3Data(diff.stream()
          .map(d -> Tuple.of(d.toNative(), resultsByS3Key.get(d)._3.toStringUtf8())).collect(Collectors.toList())));
    }

    return resultsByS3Key.entrySet().stream().map(
        r -> Tuple.of(r.getValue()._1, r.getValue()._2, blobData.get(r.getKey()).map(ByteString::copyFrom).toNative()));

  }

  @Override
  public Optional<Long> getLatestOffset() {
    return txLog.getLatestOffset();
  }

  @Override
  public UUID begin() throws StoreWriteException {
    return txLog.begin();
  }

  @Override
  public void sendEvent(final UUID id, final ByteString data) throws StoreWriteException {
    var bytes = data.toByteArray();
    var hash = hashFn.apply(bytes);

    var hexKey = DatatypeConverter.printHexBinary(hash);
    blobs.put(Key.of(hexKey), Value.of(bytes));
    txLog.sendEvent(id, ByteString.copyFrom(hash));
  }

  @Override
  public Long commit(final UUID txId) throws StoreWriteException {
    return txLog.commit(txId);
  }

  @Override
  public void abort(final UUID txId) throws StoreWriteException {
    txLog.abort(txId);
  }
}
