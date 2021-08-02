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
package com.blockchaintp.daml.address;

import com.daml.ledger.participant.state.kvutils.DamlKvutils;
import com.daml.ledger.participant.state.kvutils.Raw;
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy;

/**
 *
 */
public final class QldbIdentifier implements Identifier {
  /**
   *
   */
  public QldbIdentifier() {
  }

  @Override
  public Raw.StateKey serializeStateKey(final DamlKvutils.DamlStateKey key) {
    return DefaultStateKeySerializationStrategy.serializeStateKey(key);
  }

  @Override
  public DamlKvutils.DamlStateKey deserializeStateKey(final Raw.StateKey input) {
    return DefaultStateKeySerializationStrategy.deserializeStateKey(input);
  }
}
