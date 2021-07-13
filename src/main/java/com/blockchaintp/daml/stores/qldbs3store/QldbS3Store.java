package com.blockchaintp.daml.stores.qldbs3store;

import com.amazon.ion.IonSystem;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

public class QldbS3Store implements TransactionLog<ByteString, ByteString> {

  private final QldbStore qldb;
  private final S3Store s3;
  private final StoreReader<ByteString, ByteString> reader;
  private final IonSystem ion;
  private final UnaryOperator<byte[]> hashFn;

  public QldbS3Store(StoreReader<ByteString, ByteString> reader, QldbStore qldb, S3Store s3, IonSystem ion, UnaryOperator<byte[]> hashFn) {
    this.reader = reader;
    this.qldb = qldb;
    this.s3 = s3;
    this.ion = ion;
    this.hashFn = hashFn;
  }


  @Override
  public void put(Key<ByteString> key, Value<ByteString> value) throws StoreWriteException {
    var bytes = value.toNative().toByteArray();
    var hash = hashFn.apply(bytes);
    var qldbKey = ion.singleValue(key.toNative().toStringUtf8());

    s3.put(
      new Key<>(DatatypeConverter.printHexBinary(hash)),
      new Value<>(bytes)
    );

    var ionStruct = ion.newEmptyStruct();
    ionStruct.add("id", qldbKey);
    ionStruct.add("hash", ion.newBlob(hash));
    qldb.put(
      new Key<>(qldbKey),
      new Value<>(ionStruct)
    );
  }

  @Override
  public void put(List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs) throws StoreWriteException {
    for (var kv : listOfPairs) {
      this.put(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void sendEvent(String topic, String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendEvent(List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Value<ByteString>> get(Key<ByteString> key) throws StoreReadException {
    return reader.get(key);
  }

  @Override
  public Map<Key<ByteString>, Value<ByteString>> get(List<Key<ByteString>> listOfKeys) throws StoreReadException {
    return reader.get(listOfKeys);
  }
}
