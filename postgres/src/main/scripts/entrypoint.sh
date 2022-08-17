#!/bin/sh
# Copyright 2019 Blockchain Techology Partners
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}
POSTGRES_HOST=${POSTGRES_HOST:-postgres}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_DB=${POSTGRES_DB:-damlpg_ng}

INDEX_POSTGRES_USER=${INDEX_POSTGRES_USER:-$POSTGRES_USER}
INDEX_POSTGRES_PASSWORD=${INDEX_POSTGRES_PASSWORD:-$POSTGRES_PASSWORD}
INDEX_POSTGRES_HOST=${INDEX_POSTGRES_HOST:-$POSTGRES_HOST}
INDEX_POSTGRES_PORT=${INDEX_POSTGRES_PORT:-$POSTGRES_PORT}

LISTEN_ADDRESS=${LISTEN_ADDRESS:-"0.0.0.0"}

JAVA_OPTS=${JAVA_OPTS:-""}

LEDGER_ID=${LEDGER_ID:?"LEDGER_ID must be set!"}
DAML_GRPC_PORT=${DAML_GRPC_PORT:-39000}

PARTICIPANT_ID=${PARTICIPANT_ID:-"participant"}

POSTGRES_JDBC_URL=${POSTGRES_JDBC_URL:-"jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?user=${POSTGRES_USER}&password=${POSTGRES_PASSWORD}"}

PARTICIPANT_ARG="participant-id=${PARTICIPANT_ID}"
PARTICIPANT_ARG="${PARTICIPANT_ARG},address=${LISTEN_ADDRESS},port=${DAML_GRPC_PORT}"
if [ -n "${INDEX_POSTGRES_DB}" ] || [ -n "${INDEX_JDBC_URL}" ]; then
  INDEX_JDBC_URL=${INDEX_JDBC_URL:-"jdbc:postgresql://${INDEX_POSTGRES_HOST}:${INDEX_POSTGRES_PORT}/${INDEX_POSTGRES_DB}?user=${INDEX_POSTGRES_USER}&password=${INDEX_POSTGRES_PASSWORD}"}
  PARTICIPANT_ARG="${PARTICIPANT_ARG},server-jdbc-url=${INDEX_JDBC_URL}"
fi

# shellcheck disable=SC2086,SC2046,SC2068
exec java ${JAVA_OPTS} -jar daml-on-postgres*.jar \
  --txlogstore "$POSTGRES_JDBC_URL" \
  --participant "${PARTICIPANT_ARG}" \
  $@ \
  $(ls /opt/digitalasset/dar/*.dar 2>/dev/null)
