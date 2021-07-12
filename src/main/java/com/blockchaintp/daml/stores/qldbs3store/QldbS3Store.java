package com.blockchaintp.daml.stores.qldbblobstore;

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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

public class QldbBlobStore implements TransactionLog {

  private QldbStore qldb;
  private S3Store s3;
  private StoreReader reader;
  private IonSystem ion;
  private UnaryOperator<byte[]> hashFn;

  public QldbBlobStore(StoreReader reader, QldbStore qldb, S3Store s3, IonSystem ion, UnaryOperator<byte[]> hashFn) {
    this.reader = reader;
    this.qldb = qldb;
    this.s3 = s3;
    this.ion = ion;
    this.hashFn = hashFn;
  }


  @Override
  public <K, V> void put(Key<K> key, Value<V> value) throws StoreWriteException {
    var castKey = (ByteString) key.toNative();
    var castValue = (ByteString) value.toNative();
    var hash = ByteString.copyFrom(hashFn.apply(castValue.toByteArray()));

    s3.put(new Key(hash.toString()),new Value(castValue.toByteArray()));

    var ionStruct = ion.newEmptyStruct();
    ionStruct.add("id", ion.singleValue(castKey.toString()));
    ionStruct.add("hash",ion.singleValue(castValue.toByteArray()));
    qldb.put(
      new Key(ion.singleValue(hash.toString())),
      new Value(ionStruct)
    );
  }

  @Override
  public <K, V> void put(List<Map.Entry<Key<K>, Value<V>>> listOfPairs) throws StoreWriteException {
    for (var kv: listOfPairs) {
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
  public <K, V> Optional<Value<V>> get(Key<K> key, Class<V> valueClass) throws StoreReadException {
    return reader.get(key,valueClass);
  }

  @Override
  public <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys, Class<V> valueClass) throws StoreReadException {
    return reader.get(listOfKeys,valueClass);
  }
}
