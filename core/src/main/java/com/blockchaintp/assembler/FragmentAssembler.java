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
package com.blockchaintp.assembler;

import java.util.List;

/**
 * A FragmentAssembler takes items of type O and breaks them up in to fragments of type F. It can
 * also reconstruct those fragments.
 *
 * @param <O>
 *          the type of the original un-fragmented data
 * @param <F>
 *          the type of the fragments
 */
public interface FragmentAssembler<O, F> {

  /**
   * Break the original data into fragments.
   *
   * @param original
   * @return a list of fragments
   */
  List<F> fragment(O original);

  /**
   * Assemble the fragments into the original data.
   *
   * @param fragments
   * @return the original data
   * @throws InvalidAssemblage
   *           if the fragments do not form a valid assemblage
   * @throws IncompleteAssemblage
   *           if the fragments do not form a complete assemblage
   */
  O assemble(List<F> fragments) throws InvalidAssemblage, IncompleteAssemblage;
}
