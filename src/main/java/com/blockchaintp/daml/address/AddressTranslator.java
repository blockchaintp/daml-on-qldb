package com.blockchaintp.daml.address;

import java.util.List;

/**
 * An AddressTranslator can convert an identifier of type A to an address or
 * type B extending NativeAddress, and vice versa.
 * @param <A> the type of the identifier
 * @param <B> the type of the native address
 */
public interface AddressTranslator<A extends Identifier, B extends LedgerAddress> {

  /**
   * Translate the provided identifiers into ledger addresses.
   * @param identifiers
   * @return the list of equivalent ledger addresses
   */
  List<B> translate(List<A> identifiers);

  /**
   * Interpret the provided ledger addresses into identifiers.
   * @param addresses
   * @return the list of equivalent identifiers
   */
  List<A> interpret(List<B> addresses);

}
