package com.blockchaintp.daml.model;

import javax.xml.bind.DatatypeConverter;

import com.blockchaintp.daml.exception.NonRecoverableErrorException;

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
      throw new NonRecoverableErrorException(nsae);
    }
  }
}
