package com.blockchaintp.daml.stores.layers;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import javax.xml.bind.DatatypeConverter;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.daml.stores.service.Value;
import com.google.protobuf.ByteString;

public class SplitStore implements TransactionLog<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<IonValue, IonStruct> qldb;
  private final Store<String, byte[]> s3;
  private final StoreReader<ByteString, ByteString> reader;
  private final IonSystem ion;
  private final UnaryOperator<byte[]> hashFn;

  public SplitStore(final boolean s3Index, final StoreReader<ByteString, ByteString> indexReader,
      final TransactionLog<IonValue, IonStruct> txLog, final Store<String, byte[]> blobStore, final IonSystem sys,
      final UnaryOperator<byte[]> hashingFn) {
    this.writeS3Index = s3Index;
    this.reader = indexReader;
    this.qldb = txLog;
    this.s3 = blobStore;
    this.ion = sys;
    this.hashFn = hashingFn;
  }

  @Override
  public final void put(final Key<ByteString> key, final Value<ByteString> value) throws StoreWriteException {
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
    qldb.put(new Key<>(qldbKey), new Value<>(ionStruct));
  }

  private void writeBlob(final byte[] bytes, final String hexKey) throws StoreWriteException {
    s3.put(new Key<>(hexKey), new Value<>(bytes));
  }

  private void writeIndexedBlob(final Key<ByteString> key, final byte[] bytes, final byte[] hash, final String hexKey)
      throws StoreWriteException {
    s3.put(Arrays.asList(new AbstractMap.SimpleEntry<>(new Key<>(hexKey), new Value<>(bytes)),
        new AbstractMap.SimpleEntry<>(new Key<>(String.format("index/%s", key.toNative().toStringUtf8())),
            new Value<>(hash))));
  }

  @Override
  public final void put(final List<Map.Entry<Key<ByteString>, Value<ByteString>>> listOfPairs)
      throws StoreWriteException {
    for (var kv : listOfPairs) {
      this.put(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public final void sendEvent(final String topic, final String data) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void sendEvent(final List<Map.Entry<String, String>> listOfPairs) throws StoreWriteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final Optional<Value<ByteString>> get(final Key<ByteString> key) throws StoreReadException {
    return reader.get(key);
  }

  @Override
  public final Map<Key<ByteString>, Value<ByteString>> get(final List<Key<ByteString>> listOfKeys)
      throws StoreReadException {
    return reader.get(listOfKeys);
  }
}
