/*
 * Copyright 2020-2021 Blockchain Technology Partners
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

final case class ExtraConfig(
    createAws: Boolean,
    keystore: String,
    region: String,
    maxS3ClientConcurrency: Int,
    maxOpsPerBatch: Int,
    maxOutStandingBatches: Int,
    logLevel: String,
    authType: String,
    secret: String,
    jwksUrl: String
)

object ExtraConfig {

  val default =
    ExtraConfig(
      createAws = false, keystore = "/etc/daml/keystore", maxOpsPerBatch = "1000".toInt,
      maxOutStandingBatches = "2".toInt, logLevel = "info", authType = "none", secret = "", jwksUrl = "",
      region = "US_EAST_2", maxS3ClientConcurrency = 30
    )
}
