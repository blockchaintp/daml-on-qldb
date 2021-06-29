import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QLDBStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.*;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

class QLDBStoreUnitTest {

  @Test
  void session_aquisition_failures_raises_reasonable_exceptions() {
    var closedDriver = mock(QldbDriver.class);
    when(closedDriver.execute(any(Executor.class)))
      .thenThrow(QldbDriverException.create(Errors.DRIVER_CLOSED.get()));

    var closedStore = new QLDBStore(closedDriver, "", "", IonSystemBuilder.standard().build());

    final var ion = IonSystemBuilder.standard().build();

    var qldbStoreReadException = Assertions.assertThrows(StoreReadException.class,
      () -> closedStore.get(new Key(ion.singleValue("identity"))));
  }
}
