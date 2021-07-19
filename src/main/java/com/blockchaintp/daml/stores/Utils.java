package com.blockchaintp.daml.stores;

import com.blockchaintp.daml.stores.exception.NoSHA512SupportException;

import javax.xml.bind.DatatypeConverter;
import com.blockchaintp.exception.NoSHA512SupportException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Useful utilities.
 */
public final class Utils {

  private Utils() {
  }

  /**
   * Return a hex string representation of a SHA-512 hash for the provided byte array data.
   *
   * @param data array of byte data
   * @return the hash
   */
  public static String hash512(final byte[] data) {
    try {
      var messageDigest = MessageDigest.getInstance("SHA-512");

      messageDigest.update(data);

      byte[] digest = messageDigest.digest();
      return DatatypeConverter.printHexBinary(digest).toLowerCase();
    } catch (NoSuchAlgorithmException nsae) {
      throw new NoSHA512SupportException(nsae);
    }
  }
}
