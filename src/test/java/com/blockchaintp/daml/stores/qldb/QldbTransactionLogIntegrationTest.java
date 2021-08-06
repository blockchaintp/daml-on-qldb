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
package com.blockchaintp.daml.stores.qldb;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.blockchaintp.daml.stores.exception.StoreWriteException;
import com.blockchaintp.daml.stores.resources.QldbResources;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.qldb.QldbDriver;

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

class QldbTransactionLogIntegrationTest {

  private QldbTransactionLog txLog;
  private IonSystem ionSystem;
  private QldbResources resources;

  @BeforeEach
  final void establishStore() throws StoreWriteException {
    String ledger = UUID.randomUUID().toString().replace("-", "");

    final var sessionBuilder = QldbSessionClient.builder().region(Region.EU_WEST_2)
        .credentialsProvider(DefaultCredentialsProvider.builder().build());

    this.ionSystem = IonSystemBuilder.standard().build();

    final var driver = QldbDriver.builder().ledger(ledger).sessionClientBuilder(sessionBuilder).ionSystem(ionSystem)
        .build();

    this.resources = new QldbResources(
        QldbClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).region(Region.EU_WEST_2).build(),
        ledger);

    final var storeBuilder = QldbTransactionLog.forDriver(driver).tablePrefix("qldbtxintegrationtest");

    this.txLog = storeBuilder.build();

    resources.destroyResources();
    resources.ensureResources();
  }

  @AfterEach
  final void dropStore() {
    resources.destroyResources();
  }

  @Test
  final void committed_transactions_are_read_in_commit_order() throws StoreWriteException {
    var ids = new ArrayList<UUID>();
    for (var i = 0; i != 30; i++) {
      var id = txLog.begin();
      ids.add(id);

      txLog.sendEvent(id, ByteString.copyFromUtf8("testdata"));

      txLog.commit(id);
    }

    var aborted = txLog.begin();
    txLog.sendEvent(aborted, ByteString.copyFromUtf8("aborted"));
    txLog.abort(aborted);

    var stream = txLog.from(Optional.of(0L));

    Assertions.assertIterableEquals(ids, stream.take(30).map(x -> x._2).collect(Collectors.toList()).blockingGet());
  }

}
