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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import io.reactivex.rxjava3.core.Observable;

/**
 * TransactionLog composing a transaction log and an S3 store to keep large values outside of the
 * transaction log.
 */
public final class SplitTransactionLog implements TransactionLog<UUID, ByteString, Long> {
  private final TransactionLog<UUID, ByteString, Long> txLog;
  private final Store<String, byte[]> blobs;
  private final UnaryOperator<byte[]> hashFn;

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

  @Override
  public Observable<Map.Entry<UUID, ByteString>> from(final Optional<Long> offset) {
    return txLog.from(offset).map(r -> {
      var s3Key = Key.of(DatatypeConverter.printHexBinary(r.getValue().toByteArray()));
      var withS3data = blobs.get(s3Key).map(v -> Map.entry(r.getKey(), v.map(x -> ByteString.copyFrom(x)).toNative()));

      /// If we are missing underlying s3 data then this is a serious problem
      if (withS3data.isEmpty()) {
        throw new StoreReadException(SpltStoreException.missingS3Data(s3Key.toString(), r.getKey().toString()));
      }

      return withS3data.get();
    });
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
