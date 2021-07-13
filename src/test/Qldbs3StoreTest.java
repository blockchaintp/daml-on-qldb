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
import org.mockito.ArgumentMatchers;

import java.nio.charset.Charset;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class Qldbs3StoreTest {
  @Test
  void put_enters_hash_into_qldb_after_storing_in_s3() throws StoreWriteException {
    var ion = IonSystemBuilder.standard().build();
    var qldbStore = mock(QldbStore.class);
    var s3Store = mock(S3Store.class);
    var qldbS3Store = new QldbS3Store(
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

    verify(qldbStore)
      .put(
        new Key<>(ion.singleValue("DamlKey")),
        new Value<>(ionS)
      );

    verify(s3Store)
      .put(
        ArgumentMatchers.eq(new Key<>("6F6B")),
        ArgumentMatchers.any(Value.class)
      );
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
