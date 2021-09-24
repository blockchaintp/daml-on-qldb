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

/**
 *
 */
public final class QldbIdentifier implements Identifier {
  private final DamlKvutils.DamlStateKey data;

  /**
   *
   * @param theData
   */
  public QldbIdentifier(final DamlKvutils.DamlStateKey theData) {
    data = theData;
  }

  @Override
  public DamlKvutils.DamlStateKey toKey() {
    return data;
  }
}
