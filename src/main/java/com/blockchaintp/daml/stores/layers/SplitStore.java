package com.blockchaintp.daml.stores.layers;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.*;
import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 * A Store that keeps its values in a blob store which is keyed by the hash of
 * the value. That original key is stored along with that hash in the
 * TransactionLog.
 */
public class SplitStore implements TransactionLog<ByteString, ByteString> {

  private final boolean writeS3Index;
  private final Store<ByteString, ByteString> qldb;
  private final Store<String, byte[]> s3;
  private final StoreReader<ByteString, ByteString> reader;
  private final UnaryOperator<byte[]> hashFn;

  /**
   * Constructs a new SplitStore.
   *
   * @param s3Index     whether to allow fetching from the blob store if the value has been verified
   * @param indexReader a reader for the index, verified or not
   * @param txLog       the TransactionLog to use
   * @param blobStore   the blob store to use
   * @param hashingFn   the hash function to use
   */
  public SplitStore(final boolean s3Index, final StoreReader<ByteString, ByteString> indexReader,
                    final TransactionLog<ByteString, ByteString> txLog, final Store<String, byte[]> blobStore,
                    final UnaryOperator<byte[]> hashingFn) {
    this.writeS3Index = s3Index;
    this.reader = indexReader;
    this.qldb = txLog;
    this.s3 = blobStore;
    this.hashFn = hashingFn;
  }

  @Override
  public void put(Key<ByteString> key, Value<ByteString> value) throws StoreWriteException {
    var bytes = value.toNative().toByteArray();
    var hash = hashFn.apply(bytes);

    var hexKey = DatatypeConverter.printHexBinary(hash);

    if (writeS3Index) {
      writeIndexedBlob(key, bytes, hash, hexKey);
    } else {
      writeBlob(bytes, hexKey);
    }

    qldb.put(
      key,
      Value.of(ByteString.copyFrom(bytes))
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
