package com.blockchaintp.daml.stores.resilience;

import com.blockchaintp.daml.stores.LRUCache;
import com.blockchaintp.daml.stores.StubStore;
import com.blockchaintp.daml.stores.exception.StoreReadException;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.service.Key;
import com.blockchaintp.daml.stores.service.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;

@SuppressWarnings("OptionalGetWithoutIsPresent")
class CacheTest {
  private static final int CACHE_SIZE = 600;
  private LRUCache<String, String> cache;

  @BeforeEach
  void establish_cache() {
    this.cache = new LRUCache<>(CACHE_SIZE);
  }

  @Test
  void scalar_get_reads_through() throws StoreWriteException, StoreReadException {

    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cache.put(new Key<>("cache"), new Value<>("cache"));
    Assertions.assertEquals(new Value<>("cache"), cache.get(new Key<>("cache")));

    cache.put(new Key<>("primed"), new Value<>("primed"));
    stubStore.put(new Key<>("readthrough"), new Value<>("readthrough"));

    Assertions.assertEquals(new Value<>("readthrough"), cachedStore.get(new Key<>("readthrough")).get());

    Assertions.assertNotNull(cache.get(new Key<>("readthrough")));

    Assertions.assertEquals("primed", cachedStore.get(new Key<>("primed")).get().toNative());

  }

  @Test
  void scalar_put_adds_to_cache() throws StoreWriteException, StoreReadException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cachedStore.put(new Key<>("writethrough"), new Value<>("writethrough"));

    Assertions.assertEquals("writethrough", stubStore.get(new Key<>("writethrough")).get().toNative());

    Assertions.assertNotNull(cache.get(new Key<>("writethrough")));
  }

  @Test
  void batch_get_only_reads_through_missed_items() throws StoreReadException, StoreWriteException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cache.put(new Key<>("primed"), new Value<>("primed"));
    stubStore.put(new Key<>("readthrough"), new Value<>("readthrough"));

    Assertions.assertIterableEquals(Arrays.asList(new Value<>("primed"), new Value<>("readthrough")),
      cachedStore.get(Arrays.asList(new Key<>("primed"), new Key<>("readthrough"))).values());
  }

  @Test
  void batch_put_adds_to_cache() throws StoreWriteException, StoreReadException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cachedStore
      .put(Arrays.asList(new AbstractMap.SimpleEntry<>(new Key<>("writethrough1"), new Value<>("writethrough1")),
        new AbstractMap.SimpleEntry<>(new Key<>("writethrough2"), new Value<>("writethrough2"))));

    Assertions.assertEquals("writethrough1", stubStore.get(new Key<>("writethrough1")).get().toNative());

    Assertions.assertEquals("writethrough2", stubStore.get(new Key<>("writethrough2")).get().toNative());

    Assertions.assertNotNull(cache.get(new Key<>("writethrough1")));
    Assertions.assertNotNull(cache.get(new Key<>("writethrough2")));
  }
}
