package com.blockchaintp.daml.stores.qldbs3store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.xml.bind.DatatypeConverter;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.google.protobuf.ByteString;

/**
 * Checks QLDB contains the hash before reading value from s3
 */
public class VerifiedReader implements StoreReader<ByteString, ByteString> {

  private final TransactionLog<IonValue, IonStruct> qldb;
  private final Store<String, byte[]> blobStore;
  private final IonSystem ion;

  public VerifiedReader(final TransactionLog<IonValue, IonStruct> txlog, final Store<String, byte[]> blobStore,
      final IonSystem sys) {
    this.qldb = txlog;
    this.blobStore = blobStore;
    this.ion = sys;
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    var qldbRef = qldb.get(new Key<>(ion.singleValue(key.toNative().toStringUtf8())));

    if (qldbRef.isPresent()) {
      var hashField = (IonBlob) qldbRef.get().toNative().get("hash");

      Optional<Value<byte[]>> s3Val = blobStore.get(new Key<>(DatatypeConverter.printHexBinary(hashField.getBytes())));

      return s3Val.map(x -> new Value<>(ByteString.copyFrom(x.toNative())));
    } else {
      return Optional.empty();
    }
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
