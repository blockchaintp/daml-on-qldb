package com.blockchaintp.daml.stores.layers;

import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reads straight from s3 using an index.
 */
public class UnVerifiedReader implements StoreReader<ByteString, ByteString> {
  private final Store<String, byte[]> blobs;

  /**
   * Construct an unverified reader around the provided store.
   *
   * @param blobs The underlying store
   */
  public UnVerifiedReader(Store<String, byte[]> blobs) {
    this.blobs = blobs;
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {

    var s3Index = blobs.get(new Key<>(String.format("index/%s",
      key.toNative().toStringUtf8())));

    if (s3Index.isPresent()) {
      return blobs.get(
        new Key<>(DatatypeConverter.printHexBinary(s3Index.get().toNative()))
      ).map(v -> v.map(ByteString::copyFrom));
    }

    return Optional.empty();
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
