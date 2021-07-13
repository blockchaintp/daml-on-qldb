package com.blockchaintp.daml.stores.qldbs3store;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.serviceinterface.*;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.*;
import java.util.function.UnaryOperator;

public class QldbS3Store implements TransactionLog<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<IonValue, IonStruct> qldb;
  private final Store<String, byte[]> s3;
  private final StoreReader<ByteString, ByteString> reader;
  private final IonSystem ion;
  private final UnaryOperator<byte[]> hashFn;

  public QldbS3Store(boolean writeS3Index, StoreReader<ByteString, ByteString> reader, Store<IonValue, IonStruct> qldb, Store<String, byte[]> s3, IonSystem ion, UnaryOperator<byte[]> hashFn) {
    this.writeS3Index = writeS3Index;
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

    var hexKey = DatatypeConverter.printHexBinary(hash);

    if (writeS3Index) {
      writeIndexedBlob(key, bytes, hash, hexKey);
    } else {
      writeBlob(bytes, hexKey);
    }

    var ionStruct = ion.newEmptyStruct();
    ionStruct.add("id", qldbKey);
    ionStruct.add("hash", ion.newBlob(hash));
    qldb.put(
      new Key<>(qldbKey),
      new Value<>(ionStruct)
    );
  }


  private void writeBlob(byte[] bytes, String hexKey) throws StoreWriteException {
    s3.put(
      new Key<>(hexKey),
      new Value<>(bytes)
    );
  }

  private void writeIndexedBlob(Key<ByteString> key, byte[] bytes, byte[] hash, String hexKey) throws StoreWriteException {
    s3.put(Arrays.asList(
      new AbstractMap.SimpleEntry<>(
        new Key<>(hexKey),
        new Value<>(bytes)
      ),
      new AbstractMap.SimpleEntry<>(
        new Key<>(String.format("index/%s",
          key.toNative().toStringUtf8()
        )),
        new Value<>(hash)
      )
    ));
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
