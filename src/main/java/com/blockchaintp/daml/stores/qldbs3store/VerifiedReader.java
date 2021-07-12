package com.blockchaintp.daml.stores.qldbblobstore;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Checks QLDB contains the hash before reading value from s3
 */
public class VerifiedReader implements StoreReader {

  private StoreReader qldb;
  private StoreReader s3;
  private IonSystem ion;

  public VerifiedReader(QldbStore qldb, S3Store s3, IonSystem ion) {
    this.qldb = qldb;
    this.s3 = s3;
    this.ion = ion;
  }

  /**
   * TODO, assert QLDB doc structure
   * @param key
   * @param valueClass
   * @param <K>
   * @param <V>
   * @return
   * @throws StoreReadException
   */
  @Override
  public <K, V> Optional<Value<V>> get(Key<K> key, Class<V> valueClass) throws StoreReadException {
    var castKey = (ByteString) key.toNative();
    var qldbRef = (Optional<Value<IonStruct>>) qldb.get(
      new Key(ion.singleValue(castKey.toString())), IonStruct.class
    );

    if (qldbRef.isPresent()) {
      var qldbStruct = qldbRef.get();
      return s3.get(
        new Key(qldbRef.get()
        .toNative()
        .get("value")),
        IonValue.class);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public <K, V> Map<Key<K>, Value<V>> get(List<Key<K>> listOfKeys, Class<V> valueClass) throws StoreReadException {
    var map = new HashMap<Key<K>,Value<V>>();
    for (var k: listOfKeys) {
      var item = this.get(k,valueClass);
      if (item.isPresent()) {
        map.put(k,item.get());
      }
    }

    return map;
  }
}
