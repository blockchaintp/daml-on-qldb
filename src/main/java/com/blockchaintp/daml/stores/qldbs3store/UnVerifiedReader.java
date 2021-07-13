package com.blockchaintp.daml.stores.qldbs3store;

import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reads straight from s3 using an index
 */
public class UnVerifiedReader implements StoreReader<ByteString, ByteString> {

  private final S3Store s3;

  public UnVerifiedReader(S3Store s3) {
    this.s3 = s3;
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {

    var s3Index = s3.get(new Key<>(String.format("index/%s",
      key.toNative().toStringUtf8())));

    if (s3Index.isPresent()) {
      return s3.get(
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
