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
import com.blockchaintp.daml.participant.CommitPayloadBuilder
import com.blockchaintp.daml.participant.ParticipantBuilder
import com.blockchaintp.daml.runtime.BuilderLedgerFactory
import com.daml.jwt.JwksVerifier
import com.daml.jwt.RSA256Verifier
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.auth.AuthServiceJWT
import com.daml.ledger.api.auth.AuthServiceWildcard
import com.daml.ledger.participant.state.kvutils.app.Config
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.participant.state.v1.TimeModel
import com.daml.ledger.resources.ResourceContext
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.resources.ProgramResource
import scopt.OptionParser

import java.nio.file.Paths
import java.time.Duration
import scala.util.Try

object Main {

  def main(args: Array[String]): Unit = {
    val runner = new Runner(
      "Qldb Ledger",
      new LedgerFactory((config: Config[ExtraConfig], builder: ParticipantBuilder[QldbIdentifier, QldbAddress]) =>
        builder.configureCommitPayloadBuilder(p => p.withNoFragmentation())
      )
    ).owner(args)
    new ProgramResource(runner).run(ResourceContext.apply)
  }

  class LedgerFactory(
      build: (
          Config[ExtraConfig],
          ParticipantBuilder[QldbIdentifier, QldbAddress]
      ) => ParticipantBuilder[QldbIdentifier, QldbAddress]
  ) extends BuilderLedgerFactory(build) {

    override def ledgerConfig(config: Config[ExtraConfig]): LedgerConfiguration =
      LedgerConfiguration(
        initialConfiguration = Configuration(
          // NOTE: Any changes the the config content here require that the
          // generation be increased
          generation = 1L,
          timeModel = TimeModel(
            avgTransactionLatency = Duration.ofSeconds(1L),
            minSkew = Duration ofSeconds 80L,
            maxSkew = Duration.ofSeconds(80L)
          ).get,
          maxDeduplicationTime = Duration.ofDays(1L)
        ),
        initialConfigurationSubmitDelay = Duration.ofSeconds(5L),
        configurationLoadTimeout = Duration.ofSeconds(30L)
      )

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

    override val defaultExtraConfig: ExtraConfig = ExtraConfig.default

    private def validatePath(path: String, message: String): Either[String, Unit] = {
      val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
      if (valid) Right(()) else Left(message)
    }

    final override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
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
        .opt[String]("max-ops-per-batch")
        .optional()
        .text("maximum number of operations per batch")
        .action { case (v, config) =>
          config.copy(
            extra = config.extra.copy(
              maxOpsPerBatch = v.toInt
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
        .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
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
      parser
        .opt[String]("max-outstanding-batches")
        .optional()
        .text("maximum number of batches outstanding")
        .action { case (v, config) =>
          config.copy(
            extra = config.extra.copy(
              maxOutStandingBatches = v.toInt
            )
          )
        }
      ()
    }
  }
}
