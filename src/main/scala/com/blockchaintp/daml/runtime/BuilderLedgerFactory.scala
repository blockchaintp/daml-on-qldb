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
package com.blockchaintp.daml.runtime

import akka.stream.Materializer
import com.blockchaintp.daml.address.Identifier
import com.blockchaintp.daml.address.LedgerAddress
import com.blockchaintp.daml.participant.ParticipantBuilder
import com.daml.ledger.participant.state.kvutils.api.KeyValueLedger
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory
import com.daml.ledger.participant.state.kvutils.app.ParticipantConfig
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext

abstract class BuilderLedgerFactory[
    Id <: Identifier,
    Address <: LedgerAddress,
    ExtraConfig
](
    val build: (Config[ExtraConfig], ParticipantBuilder[Id, Address]) => ParticipantBuilder[Id, Address]
) extends LedgerFactory[KeyValueParticipantState, ExtraConfig] {

  final override def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine
  )(implicit
      materializer: Materializer,
      logCtx: LoggingContext
  ): ResourceOwner[KeyValueParticipantState] = {
    for {
      readerWriter <- owner(config, participantConfig, engine, build)
    } yield new KeyValueParticipantState(
      readerWriter,
      readerWriter,
      createMetrics(participantConfig, config)
    )
  }

  def owner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
      build: (Config[ExtraConfig], ParticipantBuilder[Id, Address]) => ParticipantBuilder[Id, Address]
  )(implicit
      materializer: Materializer,
      logCtx: LoggingContext
  ): ResourceOwner[KeyValueLedger] = {
    new ParticipantOwner(
      engine,
      logCtx,
      config.ledgerId,
      participantConfig.participantId,
      config,
      build
    )
  }
}
