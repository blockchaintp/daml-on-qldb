/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
import java.util.Map;

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

    cache.put(Key.of("cache"), Value.of("cache"));
    Assertions.assertEquals(Value.of("cache"), cache.get(Key.of("cache")));

    cache.put(Key.of("primed"), Value.of("primed"));
    stubStore.put(Key.of("readthrough"), Value.of("readthrough"));

    Assertions.assertEquals(Value.of("readthrough"), cachedStore.get(Key.of("readthrough")).get());

    Assertions.assertNotNull(cache.get(Key.of("readthrough")));

    Assertions.assertEquals("primed", cachedStore.get(Key.of("primed")).get().toNative());

  }

  @Test
  void scalar_put_adds_to_cache() throws StoreWriteException, StoreReadException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cachedStore.put(Key.of("writethrough"), Value.of("writethrough"));

    Assertions.assertEquals("writethrough", stubStore.get(Key.of("writethrough")).get().toNative());

    Assertions.assertNotNull(cache.get(Key.of("writethrough")));
  }

  @Test
  void batch_get_only_reads_through_missed_items() throws StoreReadException, StoreWriteException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cache.put(Key.of("primed"), Value.of("primed"));
    stubStore.put(Key.of("readthrough"), Value.of("readthrough"));

    Assertions.assertIterableEquals(Arrays.asList(Value.of("primed"), Value.of("readthrough")),
        cachedStore.get(Arrays.asList(Key.of("primed"), Key.of("readthrough"))).values());
  }

  @Test
  void batch_put_adds_to_cache() throws StoreWriteException, StoreReadException {
    var stubStore = new StubStore<String, String>();
    var cachedStore = new com.blockchaintp.daml.stores.layers.Caching<>(cache, stubStore);

    cachedStore.put(Arrays.asList(new AbstractMap.SimpleEntry<>(Key.of("writethrough1"), Value.of("writethrough1")),
        Map.entry(Key.of("writethrough2"), Value.of("writethrough2"))));

    Assertions.assertEquals("writethrough1", stubStore.get(Key.of("writethrough1")).get().toNative());

    Assertions.assertEquals("writethrough2", stubStore.get(Key.of("writethrough2")).get().toNative());

    Assertions.assertNotNull(cache.get(Key.of("writethrough1")));
    Assertions.assertNotNull(cache.get(Key.of("writethrough2")));
  }
}
