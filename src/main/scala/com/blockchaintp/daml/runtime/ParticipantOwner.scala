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

import com.blockchaintp.daml.address.Identifier
import com.blockchaintp.daml.address.LedgerAddress
import com.blockchaintp.daml.participant.Participant
import com.blockchaintp.daml.participant.ParticipantBuilder
import com.daml.ledger.participant.state.kvutils.KeyValueSubmission
import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.resources.Resource
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.resources

class ParticipantOwner[ExtraConfig, Id <: Identifier, Address <: LedgerAddress](
    val ledgerConfig: LedgerConfiguration,
    val engine: Engine,
    val metrics: Metrics,
    val logCtx: LoggingContext,
    val ledgerId: LedgerId,
    val participantId: ParticipantId,
    val config: Config[ExtraConfig],
    val build: (Config[ExtraConfig], ParticipantBuilder[Id, Address]) => ParticipantBuilder[Id, Address]
) extends ResourceOwner[Participant[Id, Address]] {

  override def acquire()(implicit
      context: ResourceContext
  ): resources.Resource[ResourceContext, Participant[Id, Address]] = {
    Resource.successful(
      build(
        config,
        new ParticipantBuilder[Id, Address](ledgerId, participantId, context)
          .withInProcLedgerSubmitterBuilder(builder =>
            builder
              .withEngine(engine)
              .withMetrics(metrics)
              .withLoggingContext(logCtx)
              .withExecutionContext(context.executionContext)
              .withConfiguration(ledgerConfig.initialConfiguration)
          )
      ).build()
    )
  }
}
