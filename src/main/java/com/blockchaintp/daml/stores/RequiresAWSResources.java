package com.blockchaintp.daml.stores;

/**
 * This component can create and destroy the AWS resources it requires for
 * operation, given the correct permissions.
 */
public interface RequiresAWSResources {
  /**
   * Attempt to create required resources, without regard to exceptional behavior
   * Probably unlikely to be used outside of test scenarios or a cli tool running
   * with a different ID to a composing application, * so we do not handle
   * exceptions.
   */
  void ensureResources();

  /**
   * Attempt to destroy required resources, without regard to exceptional
   * behavior Probably unlikely to be used outside of test scenarios or a cli
   * tool running with a different ID to a composing application, * so we do not
   * handle exceptions.
   */
  void destroyResources();
}
