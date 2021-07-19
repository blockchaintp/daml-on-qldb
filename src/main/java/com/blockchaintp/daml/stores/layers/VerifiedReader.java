package com.blockchaintp.daml.stores.layers;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.service.*;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Checks QLDB contains the hash before reading value from s3.
 */
public class VerifiedReader implements StoreReader<ByteString, ByteString> {

  private final TransactionLog<ByteString, ByteString> txLog;
  private final Store<String, byte[]> blobStore;

  /**
   * Construct a VerifiedReader around the provided stores.
   *
   * @param txlog the transaction log which masters the K->Hash map.
   * @param blobs the blob store which masters the Hash->Value map.
   */
  public VerifiedReader(final TransactionLog<ByteString, ByteString> txlog, final Store<String, byte[]> blobs) {
    this.txLog = txlog;
    this.blobStore = blobs;
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {
    var txRef = txLog.get(key);

    if (txRef.isPresent()) {
      Optional<Value<byte[]>> s3Val =
        blobStore.get(new Key<>(DatatypeConverter.printHexBinary(txRef.get().toNative().toByteArray())));

      return s3Val.map(x -> new Value<>(
        ByteString.copyFrom(x.toNative())));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Map<Key<ByteString>, Value<ByteString>> get(List<Key<ByteString>> listOfKeys) throws StoreReadException {
    var map = new HashMap<Key<ByteString>, Value<ByteString>>();
    for (var k : listOfKeys) {
      var item = this.get(k);
      item.ifPresent(byteStringValue -> map.put(k, byteStringValue));
    }

    return map;
  }
}
