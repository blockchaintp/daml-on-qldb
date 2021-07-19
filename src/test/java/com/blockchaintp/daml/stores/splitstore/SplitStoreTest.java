package java.com.blockchaintp.daml.stores.splitstore;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.layers.SplitStore;
import com.blockchaintp.daml.stores.layers.VerifiedReader;
import com.blockchaintp.daml.stores.service.*;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class SplitStoreTest {
  @Test
  void unindexed_put_enters_hash_into_txlog_after_storing_in_blobstore()
    throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString, ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<String, byte[]>();
    var splitStore = new SplitStore(false, mock(StoreReader.class), txLog, blobStore,
      bytes -> new byte[]{'o', 'k'});

    splitStore.put(new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
      new Value<>(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));

    var ionS = ion.newEmptyStruct();

    ionS.add("id", ion.singleValue("DamlKey"));
    ionS.add("hash", ion.newBlob(new byte[]{'o', 'k'}));

    // txlog should contain struct
    Assertions.assertEquals(ionS, txLog.get(
      Key.of(ByteString.copyFrom("DamlKey",Charset.defaultCharset()))).get().toNative());

    // blobstore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
      blobStore.get(Key.of("6F6B")).get().toNative());

  }

  @Test
  void indexed_put_enters_hash_into_txlog_after_storing_in_blobStore_and_also_stores_index()
    throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var txLog = new com.blockchaintp.daml.stores.StubStore<ByteString,ByteString>();
    var blobStore = new com.blockchaintp.daml.stores.StubStore<String, byte[]>();

    var splitStore = new SplitStore(true, mock(StoreReader.class), txLog, blobStore,
      bytes -> new byte[]{'o', 'k'});

    splitStore.put(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
      Value.of(ByteString.copyFrom("CONTENT", Charset.defaultCharset())));



    // txlog should contain struct
    Assertions.assertArrayEquals(new byte[] {'o','k'},
      txLog.get(Key.of(ByteString.copyFrom("DamlKey", Charset.defaultCharset()))).get().toNative().toByteArray());

    // blobStore Should contain blob
    Assertions.assertArrayEquals(ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
      blobStore.get(new Key<>("6F6B")).get().toNative());

    // blobStore Index should contain blob hash
    Assertions.assertArrayEquals(new byte[]{'o', 'k'}, blobStore.get(new Key<>("index/DamlKey")).get().toNative());
  }

  @Test
  void verified_reader_reads_hash_from_txlog_then_looks_up_as_key_in_blobstore() throws StoreReadException {

    var ion = IonSystemBuilder.standard().build();
    var txStore = mock(TransactionLog.class);
    var blobStore = mock(Store.class);
    var verified = new VerifiedReader(txStore, blobStore, ion);

    var txStoreResult = ion.newEmptyStruct();
    txStoreResult.add("id", ion.singleValue("DamlKey"));
    txStoreResult.add("hash", ion.newBlob(new byte[]{'o', 'k'}));

    when(txStore.get(new Key<>(ion.singleValue("DamlKey")))).thenReturn(Optional.of(new Value<>(txStoreResult)));
    when(blobStore.get(new Key<>("6F6B"))).thenReturn(Optional.of(new Value<>(new byte[]{'x'})));

    Optional<Value<ByteString>> blobStoreVal = verified
      .get(new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())));

    Assertions.assertEquals(ByteString.copyFrom(new byte[]{'x'}), blobStoreVal.get().toNative());
  }
}
