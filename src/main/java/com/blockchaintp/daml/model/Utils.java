package com.blockchaintp.daml.model;

import com.blockchaintp.daml.exception.NoSHA512SupportException;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Utils {

  private Utils() {
  }

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
