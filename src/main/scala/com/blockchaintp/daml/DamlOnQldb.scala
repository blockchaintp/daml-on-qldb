/* Copyright 2019 Blockchain Technology Partners
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
------------------------------------------------------------------------------*/
package com.blockchaintp.daml

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.codahale.metrics.SharedMetricRegistries

import scala.concurrent.duration._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthServiceJWT, AuthServiceWildcard}
import com.digitalasset.jwt.{HMAC256Verifier, JwtVerifier}
import com.digitalasset.platform.apiserver.{ApiServerConfig, StandaloneApiServer}
import com.digitalasset.platform.indexer.{IndexerConfig, StandaloneIndexerServer}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

object DamlOnQldb extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  val config = Cli
    .parse(
      args,
      "daml-on-qldb",
      "A DAML Ledger API server backed by an AWS QLDB instance.\n"
    )
    .getOrElse(sys.exit(1))

  // Initialize Akka and log exceptions in flows.
  implicit val system: ActorSystem = ActorSystem("sawtooth-daml-rpc")
  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy { e =>
        logger.error(s"Supervision caught exception: $e")
        Supervision.Stop
      }
  )
  implicit val ec: ExecutionContext = system.dispatcher

  val ledger = new QldbKvState(config.ledger,config.participantId)

  // Use the default key for this RPC to verify JWTs for now.
  def hmac256(input:String) : AuthServiceJWT = {
    val secret = input.substring(8,input.length())
    val verifier:JwtVerifier = HMAC256Verifier(secret).getOrElse(throw new RuntimeException("Unable to instantiate JWT Verifier"))
    new AuthServiceJWT(verifier)
  }

  val authService = config.auth match {
    case "wildcard" => AuthServiceWildcard
    case  s if s.matches("""hmac256=([a-zA-Z0-9])+""") => hmac256(s)
    case _ => AuthServiceWildcard
  }

  config.archiveFiles.foreach { file =>
    for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchiveFromFile(file)
    } yield ledger.uploadPackages(
      UUID.randomUUID().toString,
      dar.all,
      Some("uploaded on participant startup")
    )
  }

  val indexer = Await.result(
    StandaloneIndexerServer(
      ledger,
      IndexerConfig(config.participantId, config.jdbcUrl, config.startupMode),
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}"),
    ),
    30 second
  )
  val indexServer = Await.result(
    new StandaloneApiServer(
      ApiServerConfig(config.participantId, config.archiveFiles, config.port, config.jdbcUrl, config.tlsConfig, TimeProvider.UTC, config.maxInboundMessageSize, None),
      ledger,
      ledger,
      authService,
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}"),
    ).start(),
    60 second
  )

  val closed = new AtomicBoolean(false)

  def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) {
      indexer.close()
      indexServer.close()
      materializer.shutdown()
      val _ = system.terminate()
    }
  }

  try {
    Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
  } catch {
    case NonFatal(t) =>
      logger.error(
        "Shutting down Sandbox application because of initialization error",
        t
      )
      closeServer()
  }
}
