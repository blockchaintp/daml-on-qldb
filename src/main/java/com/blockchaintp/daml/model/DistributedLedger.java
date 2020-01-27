package com.blockchaintp.daml.model;

/**
 * Distriubuted ledger interface. Any distributed ledger implementation must support this
 * minimum set of operations.
 */
public interface DistributedLedger {
    /**
     * Records a sequence of bytes as a distributed ledger entry retrievable by a given key.
     * 
     * @param key ledger entry key
     * @param buffer ledger entry value
     */
    void putObject(final String key, final byte[] buffer);

    /**
     * Returns the sequence of bytes stored in a distributed ledger for a given key.
     * 
     * @param key ledger entry key
     * @return value stored for supplied key
     */
    byte[] getObject(final String key);

    /**
     * Returns the sequence of bytes stored in a distributed ledger for a given key
     * and optionally request that the returned entry to be cached locally.
     * 
     * @param key ledger entry key
     * @param cacheable whether or not to cache the returned value in a local cache
     * @return value stored for the key
     */
    byte[] getObject(final String key, final boolean cacheable);

    /**
     * Returns whether or not a given ledger entry exists. Implementations of this
     * method make this determination as follows:
     * <ul>
     * <li>test for existence of the entry in the local cache if caching is
     * supported
     * <li>if caching is not supported or the entry is not cached:
     * <ul>
     * <li>test for existence of the entry in the ledger if the ledger supports it,
     * otherwise
     * <li>attempt to retrieve the object and return the success or failure of the
     * request (note that this means that implementations of this method may throw
     * read failure exceptions)
     * </ul>
     * </ul>
     * 
     * @param key ledger entry key
     * @return <code>true</code> if the ledger entry exists, <code>false</code>
     * otherwise
     */
    boolean existsObject(final String key);
}
