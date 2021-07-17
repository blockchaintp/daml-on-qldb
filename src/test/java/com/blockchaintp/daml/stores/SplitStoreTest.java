package com.blockchaintp.daml.stores;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.Optional;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.google.protobuf.ByteString;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class SplitStoreTest {
  @Test
  void unindexed_put_enters_hash_into_txlog_after_storing_in_blobstore() throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new StubStore<IonValue, IonStruct>();
    var blobStore = new StubStore<String, byte[]>();
    var splitStore = new SplitStore(false, mock(StoreReader.class), txLog, blobStore, ion,
        bytes -> new byte[] { 'o', 'k' });

    splitStore.put(new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
        new Value<>(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));

    var ionS = ion.newEmptyStruct();

    ionS.add("id", ion.singleValue("DamlKey"));
    ionS.add("hash", ion.newBlob(new byte[] { 'o', 'k' }));

    // txlog should contain struct
    Assertions.assertEquals(ionS, txLog.get(new Key<>(ion.singleValue("DamlKey"))).get().toNative());

    // blobstore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
        blobStore.get(new Key<>("6F6B")).get().toNative());

  }

  @Test
  void indexed_put_enters_hash_into_txlog_after_storing_in_blobStore_and_also_stores_index()
      throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new StubStore<IonValue, IonStruct>();
    var blobStore = new StubStore<String, byte[]>();

    var splitStore = new SplitStore(true, mock(StoreReader.class), txLog, blobStore, ion,
        bytes -> new byte[] { 'o', 'k' });

    splitStore.put(new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
        new Value<>(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));

    var ionS = ion.newEmptyStruct();

    ionS.add("id", ion.singleValue("DamlKey"));
    ionS.add("hash", ion.newBlob(new byte[] { 'o', 'k' }));

    // txlog should contain struct
    Assertions.assertEquals(ionS, txLog.get(new Key<>(ion.singleValue("DamlKey"))).get().toNative());

    // blobStore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
        blobStore.get(new Key<>("6F6B")).get().toNative());

    // blobStore Index should contain blob hash
    Assertions.assertArrayEquals(new byte[] { 'o', 'k' }, blobStore.get(new Key<>("index/DamlKey")).get().toNative());

    /// TODO: Fun verifying maps with mockito - I can write an blobStore stub in 12 lines..
  }

  @Test
  void verified_reader_reads_hash_from_txlog_then_looks_up_as_key_in_blobstore() throws StoreReadException {

    var ion = IonSystemBuilder.standard().build();
    var txStore = mock(TransactionLog.class);
    var blobStore = mock(Store.class);
    var verified = new VerifiedReader(txStore, blobStore, ion);

    var txStoreResult = ion.newEmptyStruct();
    txStoreResult.add("id", ion.singleValue("DamlKey"));
    txStoreResult.add("hash", ion.newBlob(new byte[] { 'o', 'k' }));

    when(txStore.get(new Key<>(ion.singleValue("DamlKey")))).thenReturn(Optional.of(new Value<>(txStoreResult)));
    when(blobStore.get(new Key<>("6F6B"))).thenReturn(Optional.of(new Value<>(new byte[] { 'x' })));

    Optional<Value<ByteString>> blobStoreVal = verified
        .get(new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())));

    Assertions.assertEquals(ByteString.copyFrom(new byte[] { 'x' }), blobStoreVal.get().toNative());

  }

}
