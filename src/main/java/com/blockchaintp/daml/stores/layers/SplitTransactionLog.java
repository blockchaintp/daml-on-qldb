package com.blockchaintp.daml.stores.layers;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import io.reactivex.rxjava3.core.Observable;

import javax.xml.bind.DatatypeConverter;

/**
 * TransactionLog composing a transaction log and an S3 store to keep large values outside of the transaction log.
 */
public final class SplitTransactionLog implements TransactionLog<UUID, ByteString, Long> {

  private final TransactionLog<UUID, ByteString, Long> txLog;
  private final Store<String, byte[]> blobs;
  private final UnaryOperator<byte[]> hashFn;

  /**
   *
   * @param txlog
   * @param blobStore
   * @param hasher
   */
  public SplitTransactionLog(final TransactionLog<UUID, ByteString, Long> txlog, final Store<String, byte[]> blobStore, final UnaryOperator<byte[]> hasher) {
    this.txLog = txlog;
    this.blobs = blobStore;
    this.hashFn = hasher;
  }

  @Override
  public Observable<Map.Entry<UUID, ByteString>> from(final Optional<Long> offset) {
    return txLog.from(offset)
      .map(r ->
        blobs.get(Key.of(DatatypeConverter.printHexBinary(r.getValue().toByteArray())))


      );
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
