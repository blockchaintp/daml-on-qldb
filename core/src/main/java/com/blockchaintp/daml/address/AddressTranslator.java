/*
 * Copyright © 2023 Paravela Limited
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

import java.util.List;

/**
 * An AddressTranslator can convert an identifier of type A to an address or type B extending
 * NativeAddress, and vice versa.
 *
 * @param <A>
 *          the type of the identifier
 * @param <B>
 *          the type of the native address
 */
public interface AddressTranslator<A extends Identifier, B extends LedgerAddress> {

  /**
   * Translate the provided identifiers into ledger addresses.
   *
   * @param identifiers
   * @return the list of equivalent ledger addresses
   */
  List<B> translate(List<A> identifiers);

  /**
   * Interpret the provided ledger addresses into identifiers.
   *
   * @param addresses
   * @return the list of equivalent identifiers
   */
  List<A> interpret(List<B> addresses);

}
