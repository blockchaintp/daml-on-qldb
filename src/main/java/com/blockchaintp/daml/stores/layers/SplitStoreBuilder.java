package com.blockchaintp.daml.stores.layers;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.blockchaintp.daml.stores.service.Store;
import com.blockchaintp.daml.stores.service.StoreReader;
import com.blockchaintp.daml.stores.service.TransactionLog;
import com.blockchaintp.exception.NoSHA512SupportException;
import com.google.protobuf.ByteString;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

/**
 * A builder of a {@link SplitStore}.
 */
public class SplitStoreBuilder {
  private TransactionLog<ByteString, ByteString> txLog;
  private Store<String, byte[]> blobs;
  private StoreReader<ByteString, ByteString> reader;
  private UnaryOperator<byte[]> hashFn;
  private boolean writeS3Index = false;

  /**
   * Create a SplitStoreBuilder.
   *
   * @param transactionLog the transaction log to use
   * @param blobStore      the blob store to use
   */
  public SplitStoreBuilder(final TransactionLog<ByteString, ByteString> transactionLog,
                           final Store<String, byte[]> blobStore) {
    this.txLog = transactionLog;
    this.blobs = blobStore;
    this.hashFn = bytes -> {
      try {
        var messageDigest = MessageDigest.getInstance("SHA-512");

        messageDigest.update(bytes);

        return messageDigest.digest();
      } catch (NoSuchAlgorithmException nsae) {
        throw new NoSHA512SupportException(nsae);
      }
    };
    this.reader = new VerifiedReader(transactionLog, blobStore);
  }

  /**
   * Whether or not to allow verified reads.
   *
   * @param verified {@code true} to allow verified reads
   * @return the builder
   */
  public final SplitStoreBuilder verified(final boolean verified) {
    if (verified) {
      this.reader = new VerifiedReader(txLog, blobs);
    } else {
      this.reader = new UnVerifiedReader(blobs);
    }

    return this;
  }

  /**
   * Whether to write the S3 index.
   *
   * @param s3Index {@code true} to write the S3 index
   * @return the builder
   */
  public final SplitStoreBuilder withS3Index(final boolean s3Index) {
    this.writeS3Index = s3Index;

    return this;
  }

  /**
   * Use the given hash function to hash the contents of a blob.
   *
   * @param hasherFn the hash function
   * @return the builder
   */
  public final SplitStoreBuilder withHasher(final UnaryOperator<byte[]> hasherFn) {
    this.hashFn = hasherFn;

    return this;
  }

  /**
   * Build the split store.
   *
   * @return the split store
   */
  public final SplitStore build() {
    return new SplitStore(writeS3Index, reader, txLog, blobs, hashFn);
  }
}
