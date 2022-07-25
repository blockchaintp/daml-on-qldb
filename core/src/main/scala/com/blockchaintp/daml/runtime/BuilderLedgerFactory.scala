/*
 * Copyright 2021-2022 Blockchain Technology Partners
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
import com.daml.ledger.api.v1.admin.config_management_service.TimeModel
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.api.KeyValueLedger
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory
import com.daml.ledger.participant.state.kvutils.app.ParticipantConfig
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.InitialLedgerConfiguration

import java.time.Duration

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
    val metrics = createMetrics(participantConfig, config)
    for {
      readerWriter <- owner(config, metrics, participantConfig, engine, build)
    } yield new KeyValueParticipantState(
      readerWriter,
      readerWriter,
      metrics,
      false
    )
  }

  override def initialLedgerConfig(config: Config[ExtraConfig]): InitialLedgerConfiguration =
    InitialLedgerConfiguration(
      configuration = Configuration(
        generation = 1,
        timeModel = TimeModel(
          LedgerTimeModel(
            avgTransactionLatency = Duration.ofSeconds(1L),
            minSkew = Duration.ofSeconds(40L),
            maxSkew = Duration.ofSeconds(80L)
          ).get,
          maxDeduplicationTime = Duration.ofDays(1)
        ),
        initialConfigurationSubmitDelay = Duration.ofSeconds(5),
        configurationLoadTimeout = Duration.ofSeconds(10)
          delayBeforeSubmitting = Duration.ofSeconds(5)
      )
    )

  def initialLedgerConfig(config: Config[ExtraConfig]): InitialLedgerConfiguration =
    InitialLedgerConfiguration(
      configuration = Configuration(
        generation = 1,
        timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ofSeconds(1L),
          minSkew = Duration.ofSeconds(40L),
          maxSkew = Duration.ofSeconds(80L)
        ).get,
        maxDeduplicationTime = Duration.ofDays(1)
      ),
      delayBeforeSubmitting = Duration.ofSeconds(5)
    )

  def owner(
      config: Config[ExtraConfig],
      metrics: Metrics,
      participantConfig: ParticipantConfig,
      engine: Engine,
      build: (Config[ExtraConfig], ParticipantBuilder[Id, Address]) => ParticipantBuilder[Id, Address]
  )(implicit
      materializer: Materializer,
      logCtx: LoggingContext
  ): ResourceOwner[KeyValueLedger] = {
    new ParticipantOwner(
      initialLedgerConfig(config),
      engine,
      metrics,
      logCtx,
      config.ledgerId,
      participantConfig.participantId,
      config,
      build
    )
  }
}
