package com.blockchaintp.daml.stores.qldbblobstore;

import com.amazon.ion.IonSystem;
import com.blockchaintp.daml.exception.NoSHA512SupportException;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.stores.qldb.QldbStore;
import com.blockchaintp.daml.stores.s3.S3Store;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

public class QldbBlobStoreBuilder {
  private IonSystem ion;
  private QldbStore qldbStore;
  private S3Store s3Store;
  private StoreReader reader;
  private UnaryOperator<byte[]> hashFn;

  public QldbBlobStoreBuilder QLDBS3StoreBuilder(IonSystem ion, QldbStore qldbStore, S3Store s3Store) {
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
    this.reader = new VerifiedReader(qldbStore,s3Store, ion);

    return this;
  }

  public QldbBlobStoreBuilder verified() {
    this.reader = new VerifiedReader(qldbStore,s3Store, ion);

    return this;
  }

  public QldbBlobStoreBuilder withHasher(UnaryOperator<byte[]> hashFn) {
    this.hashFn = hashFn;

    return this;
  }

  public QldbBlobStore build() {
    return new QldbBlobStore(
      reader,
      qldbStore,
      s3Store,
      ion,
      hashFn);
  }
}
