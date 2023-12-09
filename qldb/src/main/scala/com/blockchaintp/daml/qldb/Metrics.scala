/*
 * Copyright © 2023 Paravela Limited
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

import com.daml.platform.configuration.MetricsReporter

import scala.concurrent.duration.Duration
import scala.concurrent.duration.NANOSECONDS
import scala.util.Try

object Metrics {
  type Setter[T, B] = (B => B, T) => T
  private case class DurationFormat(unwrap: Duration)

  // We're trying to parse the java duration first for backwards compatibility as
  // removing it and only supporting the scala duration variant would be a breaking change.
  implicit private val scoptDurationFormat: scopt.Read[DurationFormat] = scopt.Read.reads { duration =>
    Try {
      Duration.fromNanos(
        java.time.Duration.parse(duration).toNanos
      )
    }.orElse(Try {
      Duration(duration)
    }).flatMap(duration =>
      Try {
        if (!duration.isFinite)
          throw new IllegalArgumentException(s"Input duration $duration is not finite")
        else DurationFormat(Duration(duration.toNanos, NANOSECONDS))
      }
    ).get
  }

  def metricsReporterParse[C](parser: scopt.OptionParser[C])(
      metricsReporter: Setter[C, Option[MetricsReporter]],
      metricsReportingInterval: Setter[C, Duration]
  ): Unit = {
    import parser.opt

    opt[MetricsReporter]("metrics-reporter")
      .action((reporter, config) => metricsReporter(_ => Some(reporter), config))
      .optional()
      .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")

    opt[DurationFormat]("metrics-reporting-interval")
      .action((interval, config) => metricsReportingInterval(_ => interval.unwrap, config))
      .optional()
      .text("Set metric reporting interval.")

    ()
  }
}
