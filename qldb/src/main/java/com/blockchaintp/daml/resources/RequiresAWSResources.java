/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.daml.resources;

/**
 * This component can create and destroy the AWS resources it requires for operation, given the
 * correct permissions.
 */
public interface RequiresAWSResources {
  /**
   * Attempt to create required resources, without regard to exceptional behavior Probably unlikely to
   * be used outside of test scenarios or a cli tool running with a different ID to a composing
   * application, * so we do not handle exceptions.
   */
  void ensureResources();

  /**
   * Attempt to destroy required resources, without regard to exceptional behavior Probably unlikely
   * to be used outside of test scenarios or a cli tool running with a different ID to a composing
   * application, * so we do not handle exceptions.
   */
  void destroyResources();
}
