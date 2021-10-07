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
package com.blockchaintp.daml.qldb

import com.blockchaintp.daml.address.QldbAddress
import com.blockchaintp.daml.address.QldbIdentifier
import com.blockchaintp.daml.participant.ParticipantBuilder
import com.blockchaintp.daml.runtime.BuilderLedgerFactory
import com.blockshaintp.daml.stores.postgres.PostgresStore
import com.blockshaintp.daml.stores.postgres.PostgresTransactionLog
import com.daml.jwt.JwksVerifier
import com.daml.jwt.RSA256Verifier
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.auth.AuthServiceJWT
import com.daml.ledger.api.auth.AuthServiceWildcard
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata
import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.participant.state.kvutils.app.ParticipantConfig
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.platform.configuration.CommandConfiguration
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.resources.ProgramResource
import scopt.OptionParser

import scala.jdk.CollectionConverters._
import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FunctionConverters.enrichAsJavaFunction
import scala.util.Try

object Main extends App {

  val NETTY_MAX_CONCURRENCY = 100

  val runner = new Runner(
    "daml-on-qldb",
    new LedgerFactory((config: Config[ExtraConfig], builder: ParticipantBuilder[QldbIdentifier, QldbAddress]) => {

      val stateStore = PostgresStore
        .fromUrl(config.extra.txLogStore)
        .retrying(3)
        .build()

      val txLog = PostgresTransactionLog
        .fromUrl(config.extra.txLogStore)
        .migrate()
        .build();

      val inputAddressReader = (meta: CommitMetadata) =>
        meta
          .inputKeys(DefaultStateKeySerializationStrategy)
          .map(r => new QldbIdentifier(DefaultStateKeySerializationStrategy.deserializeStateKey(r)))
          .asJavaCollection
          .stream()

      val outputAddressReader = (meta: CommitMetadata) =>
        meta
          .inputKeys(DefaultStateKeySerializationStrategy)
          .map(r => new QldbIdentifier(DefaultStateKeySerializationStrategy.deserializeStateKey(r)))
          .asJavaCollection
          .stream()
      builder
        .withTransactionLogReader(txLog)
        .withInProcLedgerSubmitterBuilder(builder =>
          builder
            .withSlowCall(60000)
            .withMaxThroughput(5000)
            .withStateStore(stateStore)
            .withTransactionLogWriter(txLog)
        )
        .configureCommitPayloadBuilder(p =>
          p.withInputAddressReader(inputAddressReader.asJava)
            .withOutputAddressReader(outputAddressReader.asJava)
            .withNoFragmentation()
        )
    })
  ).owner(args)
  new ProgramResource(runner).run(ResourceContext.apply)
}

class LedgerFactory(
    build: (
        Config[ExtraConfig],
        ParticipantBuilder[QldbIdentifier, QldbAddress]
    ) => ParticipantBuilder[QldbIdentifier, QldbAddress]
) extends BuilderLedgerFactory(build) {

  override def authService(config: Config[ExtraConfig]): AuthService = {
    config.extra.authType match {
      case "none" => AuthServiceWildcard
      case "rsa256" =>
        val verifier = RSA256Verifier
          .fromCrtFile(config.extra.secret)
          .valueOr(err => sys.error(s"Failed to create RSA256 verifier for: $err"))
        AuthServiceJWT(verifier)
      case "jwks" =>
        val verifier = JwksVerifier(config.extra.jwksUrl)
        AuthServiceJWT(verifier)
    }
  }

  override def ledgerConfig(config: Config[ExtraConfig]): LedgerConfiguration =
    LedgerConfiguration.defaultLocalLedger

  override val defaultExtraConfig: ExtraConfig = ExtraConfig.default

  override def commandConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig]
  ): CommandConfiguration = {
    val DefaultTrackerRetentionPeriod: FiniteDuration = 5.minutes

    CommandConfiguration(
      inputBufferSize = 512,
      maxParallelSubmissions = 1,
      maxCommandsInFlight = 256,
      limitMaxCommandsInFlight = true,
      retentionPeriod = DefaultTrackerRetentionPeriod
    )
  }

  private def validatePath(path: String, message: String) = {
    val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (valid) Right(()) else Left(message)
  }

  final override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {

    parser
      .opt[String](name = "txlogstore")
      .required()
      .text("JDBC connection url for the tx log blob store")
      .action { case (v, config) =>
        config.copy(
          extra = config.extra.copy(
            txLogStore = v
          )
        )
      }

    parser
      .opt[String]("keystore")
      .optional()
      .text("Directory of the keystore")
      .action { case (v, config) =>
        config.copy(
          extra = config.extra.copy(
            keystore = v
          )
        )
      }

    parser
      .opt[String]("log-level")
      .optional()
      .text("set log level (warn,info,debug,trace)")
      .action { case (v, config) =>
        config.copy(
          extra = config.extra.copy(
            logLevel = v
          )
        )
      }

    parser
      .opt[String]("auth-jwt-rs256-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-rs256-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { case (v, config) =>
        config.copy(
          extra = config.extra.copy(
            secret = v,
            authType = "rsa256"
          )
        )
      }
    parser
      .opt[String]("auth-jwt-rs256-jwks")
      .optional()
      .validate(v => Either.cond(v.nonEmpty, (), "JWK server URL must be a non-empty string"))
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL"
      )
      .action { case (v, config) =>
        config.copy(
          extra = config.extra.copy(
            jwksUrl = v,
            authType = "jwks"
          )
        )
      }
    ()
  }
}
