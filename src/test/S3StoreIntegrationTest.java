import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.blockchaintp.daml.stores.s3.S3StoreResources;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.*;
import java.util.stream.Collectors;

public class S3StoreIntegrationTest {

  private S3Store store;
  private S3StoreResources resources;

  @BeforeAll
  public static void enableAwsSDKTracing() {
    var console = new ConsoleAppender(); //create appender
    //configure the appender
    var PATTERN = "**AWS** %d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.TRACE);
    console.activateOptions();
    console.setName("test");
    Logger.getRootLogger().addAppender(console);
  }

  @AfterAll
  public static void disableAWsSdkTracing() {
    Logger.getRootLogger().removeAppender("test");
  }


  @BeforeEach
  public void create_test_bucket() {
    var ledgerId = UUID.randomUUID().toString().substring(0, 10);
    var tableId = UUID.randomUUID().toString().substring(0, 10);

    SdkAsyncHttpClient httpClient =
      NettyNioAsyncHttpClient.builder()
        .maxConcurrency(100)
        .build();

    var clientBuilder = S3AsyncClient.builder()
      .httpClient(httpClient)
      .region(Region.EU_WEST_2)
      .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.resources = new S3StoreResources(
      clientBuilder.build(),
      ledgerId,
      tableId
    );

    resources.ensureResources();

    this.store = S3Store.forClient(clientBuilder)
      .forLedger(ledgerId)
      .forTable(tableId)
      .build();
  }

  @AfterEach
  public void destroy_test_bucket() {
    resources.destroyResources();
  }


  @Test
  void get_non_existent_items_returns_none() throws StoreReadException {
    Assertions.assertEquals(
      Optional.empty(),
      store.get(new Key("nothere"), byte[].class));
  }

  @Test
  public void single_item_put_and_get_are_symetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    final var k = new Key("id");
    final var v = new Value(new byte[]{1, 2, 3});

    //Insert
    store.put(k, v);

    var putValue = store.get(k, byte[].class);
    Assertions.assertArrayEquals(
      (byte[]) v.toNative(),
      (byte[]) v.toNative()
    );

    final var v2 = new Value(new byte[]{3, 2, 1});

    //Update
    store.put(k, v2);
    Optional<Value<byte[]>> justPut = store.get(k, byte[].class);
    Assertions.assertArrayEquals(
      (byte[]) v2.toNative(),
      (byte[]) justPut.get().toNative()
    );
  }


  @Test
  public void multiple_item_put_and_get_are_symetric() throws StoreWriteException, StoreReadException {
    final var id = UUID.randomUUID();
    var map = new HashMap<Key<String>, Value<byte[]>>();

    for (int i = 0; i != 40; i++) {
      final var k = new Key(String.format("id%d", i));
      final var v = new Value(new byte[]{1, 2, 3});

      map.put(k, v);
    }

    var sortedkeys = map.entrySet()
      .stream()
      .map(Map.Entry::getKey)
      .sorted(new Comparator<Key<String>>() {
        @Override
        public int compare(Key<String> o1, Key<String> o2) {
          return o1.toNative().compareTo(o2.toNative());
        }
      })
      .collect(Collectors.toList());


    // Put our initial list of values, will issue insert
    store.put(map.entrySet().stream().collect(Collectors.toList()));
    var rx = store.get(sortedkeys, byte[].class);

    compareUpserted(map, sortedkeys, rx);

    // Put it again, will issue update
    store.put(map.entrySet().stream().collect(Collectors.toList()));
    var rx2 = store.get(sortedkeys, byte[].class);

    compareUpserted(map, sortedkeys, rx2);
  }

  private void compareUpserted(HashMap<Key<String>, Value<byte[]>> map, List<Key<String>> sortedkeys, Map<Key<String>, Value<byte[]>> rx) {
    Assertions.assertIterableEquals(
      sortedkeys
        .stream()
        .map(map::get)
        .map(v -> v.toNative())
        .collect(Collectors.toList()),
      sortedkeys
        .stream()
        .map(rx::get)
        .map(k -> k.toNative())
        .collect(Collectors.toList())
    );
  }

}
