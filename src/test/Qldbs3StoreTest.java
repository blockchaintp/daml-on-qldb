import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.qldbs3store.QldbS3Store;
import com.blockchaintp.daml.stores.qldbs3store.VerifiedReader;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ALL")
public class Qldbs3StoreTest {
  @Test
  void unindexed_put_enters_hash_into_qldb_after_storing_in_s3() throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var qldbStore = new StubStore<IonValue, IonStruct>();
    var s3Store = new StubStore<String, byte[]>();
    var qldbS3Store = new QldbS3Store(
      false,
      mock(StoreReader.class),
      qldbStore,
      s3Store,
      ion,
      bytes -> new byte[]{'o', 'k'}
    );

    qldbS3Store.put(
      new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
      new Value<>(ByteString.copyFrom("CONTENT", Charset.defaultCharset()))
    );

    var ionS = ion.newEmptyStruct();

    ionS.add("id", ion.singleValue("DamlKey"));
    ionS.add("hash", ion.newBlob(new byte[]{'o', 'k'}));


    //Qldb should contain struct
    Assertions.assertEquals(
      ionS,
      qldbStore.get(new Key<>(ion.singleValue("DamlKey"))).get().toNative()
    );

    //s3 Should contain blob
    Assertions.assertArrayEquals(
      ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
      s3Store.get(new Key<>("6F6B")).get().toNative()
    );

  }


  @Test
  void indexed_put_enters_hash_into_qldb_after_storing_in_s3_and_also_stores_index() throws StoreWriteException, StoreReadException {
    var ion = IonSystemBuilder.standard().build();
    var qldbStore = new StubStore<IonValue, IonStruct>();
    var s3Store = new StubStore<String, byte[]>();

    var qldbS3Store = new QldbS3Store(
      true,
      mock(StoreReader.class),
      qldbStore,
      s3Store,
      ion,
      bytes -> new byte[]{'o', 'k'}
    );

    qldbS3Store.put(
      new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())),
      new Value<>(ByteString.copyFrom("CONTENT", Charset.defaultCharset()))
    );

    var ionS = ion.newEmptyStruct();

    ionS.add("id", ion.singleValue("DamlKey"));
    ionS.add("hash", ion.newBlob(new byte[]{'o', 'k'}));

    //Qldb should contain struct
    Assertions.assertEquals(
      ionS,
      qldbStore.get(new Key<>(ion.singleValue("DamlKey"))).get().toNative()
    );

    //s3 Should contain blob
    Assertions.assertArrayEquals(
      ByteString.copyFrom("CONTENT", Charset.defaultCharset()).toByteArray(),
      s3Store.get(new Key<>("6F6B")).get().toNative()
    );

    //S3 Index should contain blob hash
    Assertions.assertArrayEquals(
      new byte[]{'o', 'k'},
      s3Store.get(new Key<>("index/DamlKey")).get().toNative()
    );


    /// TODO: Fun verifying maps with mockito - I can write an s3 stub in 12 lines..
  }

  @Test
  void verified_reader_reads_hash_from_qldb_then_looks_up_as_key_in_s3() throws StoreReadException {

    var ion = IonSystemBuilder.standard().build();
    var qldbStore = mock(QldbStore.class);
    var s3Store = mock(S3Store.class);
    var verified = new VerifiedReader(
      qldbStore,
      s3Store,
      ion
    );

    var qldbResult = ion.newEmptyStruct();
    qldbResult.add("id", ion.singleValue("DamlKey"));
    qldbResult.add("hash", ion.newBlob(new byte[]{'o', 'k'}));

    when(qldbStore.get(new Key<>(
      ion.singleValue("DamlKey"))
    ))
      .thenReturn(Optional.of(new Value<>(qldbResult)));

    when(s3Store.get(new Key<>("6F6B")))
      .thenReturn(Optional.of(
        new Value<>(new byte[]{'x'})
      ));

    Optional<Value<ByteString>> s3Val = verified.get(
      new Key<>(ByteString.copyFrom("DamlKey", Charset.defaultCharset())
      ));

    Assertions.assertEquals(
      ByteString.copyFrom(new byte[]{'x'}),
      s3Val.get().toNative()
    );

  }

}
