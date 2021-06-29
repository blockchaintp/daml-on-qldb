import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.qldb.QLDBResources;
import com.blockchaintp.daml.stores.qldb.QLDBStore;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.qldb.QldbDriver;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Happy path testing, best done via integration or simulation
 */
public class QLDBStoreIntegrationTest {
  private QLDBStore store;
  private IonSystem ionSystem;
  private QLDBResources resources;
  private String ledger;

  @BeforeAll
  public static void enableAwsSDKTracing() {
    var console = new ConsoleAppender(); //create appender
    //configure the appender
    var PATTERN = "**AWS** %d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.INFO);
    console.activateOptions();
    console.setName("test");
    Logger.getRootLogger().addAppender(console);
  }

  @AfterAll
  public static void disableAWsSdkTracing() {
    Logger.getRootLogger().removeAppender("test");
  }

  @BeforeEach
  public void establishStore() throws StoreWriteException {
    this.ledger = UUID.randomUUID().toString().replace("-","");

    final var sessionBuilder = QldbSessionClient.builder()
      .region(Region.EU_WEST_2)
      .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.ionSystem = IonSystemBuilder.standard().build();

    final var driver = QldbDriver.builder()
      .ledger(ledger)
      .sessionClientBuilder(sessionBuilder)
      .ionSystem(ionSystem)
      .build();

    this.resources = new QLDBResources(
      QldbClient
        .builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(Region.EU_WEST_2)
        .build(),
      driver,
      ledger,
      "qldbstoreintegrationtest"
    );


    final var storeBuilder = QLDBStore
      .forDriver(driver)
      .ledgerName(ledger)
      .tableName("qldbstoreintegrationtest")
      .forIonSystem(ionSystem);

    this.store = storeBuilder
      .build();

    resources.destroyResources();
    resources.ensureResources();
  }

  @AfterEach
  public void dropStore() {
    resources.destroyResources();
  }

  @Test
  void get_non_existent_items_returns_null() throws StoreReadException {
    Assertions.assertNull(
      store.get(new Key(ionSystem.singleValue("nothere"))));
  }

  @Test
  public void single_item_put_and_get_are_symetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    final var k = new Key(ionSystem.singleValue(String.format("\'%s'", id)));
    final var v = new Value(ionSystem.singleValue(String.format(
      "{'id' : '%s', 'data': 232}", id)));

    //Insert
    store.put(k, v);
    Assertions.assertEquals(
      v.toString(),
      store.get(k).toString()
    );

    final var v2 = new Value(ionSystem.singleValue(String.format(
      "{'id' : '%s', 'data': 233}", id)));

    //Update
    store.put(k, v2);
    Assertions.assertEquals(
      v2.toString(),
      store.get(k).toString()
    );
  }

  @Test
  public void multiple_item_put_and_get_are_symetric() throws StoreWriteException, StoreReadException {
    final var map = new HashMap<Key<IonValue>, Value<IonValue>>();
    for (int i = 0; i < 40; i++) {
      final var id = UUID.randomUUID();
      final var k = new Key(ionSystem.singleValue(String.format("\"%s\"", id)));
      final var v = new Value(ionSystem.singleValue(String.format(
        "{\"id\" : \"%s\", 'data': 237}", id)));

      map.put(k, v);
    }


    var sortedkeys = map.entrySet()
      .stream()
      .map(Map.Entry::getKey)
      .sorted(new Comparator<Key<IonValue>>() {
        @Override
        public int compare(Key<IonValue> o1, Key<IonValue> o2) {
          return o1.toNative().toString().compareTo(o2.toNative().toString());
        }
      })
      .collect(Collectors.toList());

    // Put our initial list of values, will issue insert
    store.put(map.entrySet().stream().collect(Collectors.toList()));
    var rx = store.get(sortedkeys);

    compareUpserted(map, sortedkeys, rx);

    // Put it again, will issue update
    store.put(map.entrySet().stream().collect(Collectors.toList()));
    var rx2 = store.get(sortedkeys);

    compareUpserted(map, sortedkeys, rx2);
  }

  private void compareUpserted(HashMap<Key<IonValue>, Value<IonValue>> map, List<Key<IonValue>> sortedkeys, Map<Key<IonValue>, Value<Object>> rx) {
    Assertions.assertIterableEquals(
      sortedkeys
        .stream()
        .map(map::get)
        .map(Object::toString)
        .collect(Collectors.toList()),
      sortedkeys
        .stream()
        .map(rx::get)
        .map(Object::toString)
        .collect(Collectors.toList())
    );
  }

}
