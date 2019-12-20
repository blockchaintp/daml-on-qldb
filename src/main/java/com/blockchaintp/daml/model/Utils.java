package com.blockchaintp.daml.model;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Utils {

  private Utils() {
  }

  public static String hash512(final byte[] data) {
    String result = null;
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");

      messageDigest.update(data);


      byte[] digest = messageDigest.digest();
      result = DatatypeConverter.printHexBinary(digest).toLowerCase();

    } catch (NoSuchAlgorithmException nsae) {
      nsae.printStackTrace();
    }
    return result;
  }
}
