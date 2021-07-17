package com.blockchaintp.daml.stores;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.exception.NoSHA512SupportException;
import com.blockchaintp.daml.serviceinterface.Store;
import com.blockchaintp.daml.serviceinterface.StoreReader;
import com.blockchaintp.daml.serviceinterface.TransactionLog;
import com.google.protobuf.ByteString;

public class SplitStoreBuilder {
  private IonSystem ion;
  private TransactionLog<IonValue, IonStruct> txLog;
  private Store<String, byte[]> s3Store;
  private StoreReader<ByteString, ByteString> reader;
  private UnaryOperator<byte[]> hashFn;
  private boolean writeS3Index = false;

  public SplitStoreBuilder(final IonSystem sys, final TransactionLog<IonValue, IonStruct> transactionLog,
      final Store<String, byte[]> blobStore) {
    this.ion = sys;
    this.txLog = transactionLog;
    this.s3Store = blobStore;
    this.hashFn = bytes -> {
      try {
        var messageDigest = MessageDigest.getInstance("SHA-512");

        messageDigest.update(bytes);

        return messageDigest.digest();
      } catch (NoSuchAlgorithmException nsae) {
        throw new NoSHA512SupportException(nsae);
      }
    };
    this.reader = new VerifiedReader(transactionLog, blobStore, sys);
  }

  public final SplitStoreBuilder verified(final boolean verified) {
    if (verified) {
      this.reader = new VerifiedReader(txLog, s3Store, ion);
    } else {
      this.reader = new UnVerifiedReader(s3Store);
    }

    return this;
  }

  public final SplitStoreBuilder withS3Index(final boolean s3Index) {
    this.writeS3Index = s3Index;

    return this;
  }

  public final SplitStoreBuilder withHasher(final UnaryOperator<byte[]> hasherFn) {
    this.hashFn = hasherFn;

    return this;
  }

  public final SplitStore build() {
    return new SplitStore(writeS3Index, reader, txLog, s3Store, ion, hashFn);
  }
}
