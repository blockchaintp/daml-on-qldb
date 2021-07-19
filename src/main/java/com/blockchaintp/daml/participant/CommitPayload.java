package com.blockchaintp.daml.participant;

import java.util.Collection;
import java.util.Set;

import com.blockchaintp.daml.address.Identifier;
import com.blockchaintp.daml.protobuf.DamlTransaction;

/**
 * Represents the assembled data that may be required to submit a transaction.
 * @param <A> the type of the identifier (e.g. {@link LedgerAddress} or {@link Identifier})
 */
public class CommitPayload<A extends Identifier> {
  private DamlTransaction transaction;
  private Set<A> reads;
  private Set<A> writes;

  /**
   * @return the transaction
   */
  public DamlTransaction getTransaction() {
    return transaction;
  }

  /**
   * @param tx the transaction to set
   */
  public void setTransaction(final DamlTransaction tx) {
    this.transaction = tx;
  }

  /**
   * @return the reads
   */
  public Set<A> getReads() {
    return reads;
  }

  /**
   * @param rs the reads to set
   */
  public void addReads(final Collection<A> rs) {
    this.reads.addAll(rs);
  }
  /**
   * @return the writes
   */
  public Set<A> getWrites() {
    return writes;
  }
  /**
   * @param ws the writes to set
   */
  public void addWrites(final Collection<A> ws) {
    this.reads.addAll(ws);
    this.writes.addAll(ws);
  }

}
