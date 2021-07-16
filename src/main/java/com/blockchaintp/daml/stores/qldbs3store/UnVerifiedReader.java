package com.blockchaintp.daml.stores.qldbs3store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.google.protobuf.ByteString;

/**
 * Reads straight from s3 using an index.
 */
public class UnVerifiedReader implements StoreReader<ByteString, ByteString> {

  private final Store<String, byte[]> blobStore;

  public UnVerifiedReader(final Store<String, byte[]> blobs) {
    this.blobStore = blobs;
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {

    var s3Index = blobStore.get(new Key<>(String.format("index/%s", key.toNative().toStringUtf8())));

    if (s3Index.isPresent()) {
      return blobStore.get(new Key<>(DatatypeConverter.printHexBinary(s3Index.get().toNative())))
          .map(v -> v.map(ByteString::copyFrom));
    }

    return Optional.empty();
  }

  @Override
  public final Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys)
      throws StoreReadException {
    var map = new HashMap<Key<ByteString>, Value<ByteString>>();
    for (var k : listOfKeys) {
      var item = this.get(k);
      item.ifPresent(byteStringValue -> map.put(k, byteStringValue));
    }

    return map;
  }
}
