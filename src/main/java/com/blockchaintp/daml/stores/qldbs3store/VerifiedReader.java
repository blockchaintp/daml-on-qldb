package com.blockchaintp.daml.stores.qldbs3store;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Checks QLDB contains the hash before reading value from s3
 */
public class VerifiedReader implements StoreReader<ByteString, ByteString> {

  private final QldbStore qldb;
  private final S3Store s3;
  private final IonSystem ion;

  public VerifiedReader(QldbStore qldb, S3Store s3, IonSystem ion) {
    this.qldb = qldb;
    this.s3 = s3;
    this.ion = ion;
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {
    var qldbRef = (Optional<Value<IonStruct>>) qldb.get(
      new Key<>(ion.singleValue(key.toNative().toStringUtf8()))
    );

    if (qldbRef.isPresent()) {
      var hashField = (IonBlob) qldbRef.get()
        .toNative()
        .get("hash");

      Optional<Value<byte[]>> s3Val = s3.get(new Key<>(DatatypeConverter.printHexBinary(hashField.getBytes())));

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
