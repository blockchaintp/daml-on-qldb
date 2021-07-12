import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.Executor;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import static org.mockito.Mockito.*;

class QldbStoreUnitTest {

  @Test
  void session_aquisition_failures_raises_reasonable_exceptions() {
    var closedDriver = mock(QldbDriver.class);
    when(closedDriver.execute(any(Executor.class)))
      .thenThrow(QldbDriverException.create(Errors.DRIVER_CLOSED.get()));

    var closedStore = new QldbStore(closedDriver, "", "", IonSystemBuilder.standard().build());

    final var ion = IonSystemBuilder.standard().build();

    var qldbStoreReadException = Assertions.assertThrows(StoreReadException.class,
      () -> closedStore.get(new Key(ion.singleValue("identity")), IonValue.class));
  }
}
