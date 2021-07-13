package com.blockchaintp.daml.stores.qldbs3store;

import com.amazon.ion.IonSystem;
import com.blockchaintp.daml.exception.NoSHA512SupportException;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.s3.S3Store;
import com.google.protobuf.ByteString;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

public class QldbS3StoreBuilder {
  private IonSystem ion;
  private QldbStore qldbStore;
  private S3Store s3Store;
  private StoreReader<ByteString, ByteString> reader;
  private UnaryOperator<byte[]> hashFn;

  public QldbS3StoreBuilder QLDBS3StoreBuilder(IonSystem ion, QldbStore qldbStore, S3Store s3Store) {
    this.ion = ion;
    this.qldbStore = qldbStore;
    this.s3Store = s3Store;
    this.hashFn = bytes -> {
      try {
        var messageDigest = MessageDigest.getInstance("SHA-512");

        messageDigest.update(bytes);

        return messageDigest.digest();
      } catch (NoSuchAlgorithmException nsae) {
        throw new NoSHA512SupportException(nsae);
      }
    };
    this.reader = new VerifiedReader(qldbStore, s3Store, ion);

    return this;
  }

  public QldbS3StoreBuilder verified() {
    this.reader = new VerifiedReader(qldbStore, s3Store, ion);

    return this;
  }

  public QldbS3StoreBuilder withHasher(UnaryOperator<byte[]> hashFn) {
    this.hashFn = hashFn;

    return this;
  }

  public QldbS3Store build() {
    return new QldbS3Store(
      reader,
      qldbStore,
      s3Store,
      ion,
      hashFn);
  }
}