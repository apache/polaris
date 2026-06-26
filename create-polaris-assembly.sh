#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# create-polaris-assembly.sh – Build a custom Apache Polaris server assembly
# with a user-selected set of plugins and extra dependencies.
#
# Usage:
#   ./create-polaris-assembly.sh [OPTIONS]
#
# Options:
#   --auth <list>        Comma-separated auth plugins to include.
#                        Supported: ranger, opa
#                        Example: --auth ranger,opa
#
#   --federation <list>  Comma-separated federation (non-REST catalog) plugins.
#                        Supported: hadoop, hive, bigquery
#                        Example: --federation hadoop,hive
#
#   --dep <coord>        Extra Maven dependency in GROUP:ARTIFACT:VERSION form.
#                        Repeat the flag to add multiple dependencies.
#                        Example: --dep com.mysql:mysql-connector-j:9.0.0
#
#   --jar <path>         Absolute path to an extra JAR file to bundle.
#                        Repeat the flag to add multiple JARs.
#                        Example: --jar /opt/plugins/my-plugin.jar
#
#   --package            After building, create a tar.gz in
#                        runtime/custom-assembly/build/distributions/.
#
#   --output <dir>       Copy the built quarkus-app to this directory.
#                        If combined with --package, copies the tar.gz instead.
#
#   --gradle-args <str>  Extra arguments forwarded verbatim to Gradle.
#                        Example: --gradle-args "--no-daemon --info"
#
#   -h, --help           Show this help message and exit.
#
# Examples:
#   # Standard server with Ranger auth and Hadoop federation
#   ./create-polaris-assembly.sh --auth ranger --federation hadoop
#
#   # With MySQL JDBC driver added to the classpath
#   ./create-polaris-assembly.sh --dep com.mysql:mysql-connector-j:9.0.0
#
#   # All auth plugins + BigQuery federation, packaged as tar.gz
#   ./create-polaris-assembly.sh --auth ranger,opa --federation bigquery --package
#
#   # Add a proprietary plugin JAR, copy result to /opt/polaris-custom
#   ./create-polaris-assembly.sh --jar /opt/plugins/acme-auth.jar --output /opt/polaris-custom

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ─── Argument parsing ─────────────────────────────────────────────────────────

AUTH_PLUGINS=""
FEDERATION_PLUGINS=""
MAVEN_DEPS=()
EXTRA_JARS=()
DO_PACKAGE=false
OUTPUT_DIR=""
EXTRA_GRADLE_ARGS=""

usage() {
  cat <<'EOF'
Usage:
  ./create-polaris-assembly.sh [OPTIONS]

Options:
  --auth <list>        Comma-separated auth plugins to include.
                       Supported: ranger, opa
                       Example: --auth ranger,opa

  --federation <list>  Comma-separated federation (non-REST catalog) plugins.
                       Supported: hadoop, hive, bigquery
                       Example: --federation hadoop,hive

  --dep <coord>        Extra Maven dependency in GROUP:ARTIFACT:VERSION form.
                       Repeat the flag to add multiple dependencies.
                       Example: --dep com.mysql:mysql-connector-j:9.0.0

  --jar <path>         Absolute or relative path to an extra JAR file to bundle.
                       Repeat the flag to add multiple JARs.
                       Example: --jar /opt/plugins/my-plugin.jar

  --package            After building, create a tar.gz in
                       runtime/custom-assembly/build/distributions/.

  --output <dir>       Copy the built quarkus-app to this directory.
                       If combined with --package, copies the tar.gz instead.

  --gradle-args <str>  Extra arguments forwarded verbatim to Gradle.
                       Example: --gradle-args "--no-daemon --info"

  -h, --help           Show this help message and exit.

Examples:
  # Standard server with Ranger auth and Hadoop federation
  ./create-polaris-assembly.sh --auth ranger --federation hadoop

  # With MySQL JDBC driver added to the classpath
  ./create-polaris-assembly.sh --dep com.mysql:mysql-connector-j:9.0.0

  # All auth plugins + BigQuery federation, packaged as tar.gz
  ./create-polaris-assembly.sh --auth ranger,opa --federation bigquery --package

  # Add a proprietary plugin JAR, copy result to /opt/polaris-custom
  ./create-polaris-assembly.sh --jar /opt/plugins/acme-auth.jar --output /opt/polaris-custom
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --auth)
      AUTH_PLUGINS="$2"
      shift 2
      ;;
    --federation)
      FEDERATION_PLUGINS="$2"
      shift 2
      ;;
    --dep)
      MAVEN_DEPS+=("$2")
      shift 2
      ;;
    --jar)
      jar_path="$(realpath "$2")"
      if [[ ! -f "$jar_path" ]]; then
        echo "ERROR: JAR file not found: $2" >&2
        exit 1
      fi
      EXTRA_JARS+=("$jar_path")
      shift 2
      ;;
    --package)
      DO_PACKAGE=true
      shift
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --gradle-args)
      EXTRA_GRADLE_ARGS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      echo "Run with --help for usage." >&2
      exit 1
      ;;
  esac
done

# ─── Build the Gradle property list ───────────────────────────────────────────

GRADLE_PROPS=()

if [[ -n "$AUTH_PLUGINS" ]]; then
  GRADLE_PROPS+=("-Pcustom.authPlugins=${AUTH_PLUGINS}")
fi

if [[ -n "$FEDERATION_PLUGINS" ]]; then
  GRADLE_PROPS+=("-Pcustom.federationPlugins=${FEDERATION_PLUGINS}")
fi

if [[ ${#MAVEN_DEPS[@]} -gt 0 ]]; then
  # Join with comma – Gradle reads this as a single property value
  deps_joined="$(IFS=','; echo "${MAVEN_DEPS[*]}")"
  GRADLE_PROPS+=("-Pcustom.mavenDeps=${deps_joined}")
fi

if [[ ${#EXTRA_JARS[@]} -gt 0 ]]; then
  jars_joined="$(IFS=','; echo "${EXTRA_JARS[*]}")"
  GRADLE_PROPS+=("-Pcustom.extraJars=${jars_joined}")
fi

# ─── Determine which Gradle task to run ───────────────────────────────────────

if [[ "$DO_PACKAGE" == true ]]; then
  GRADLE_TASK=":polaris-custom-assembly:packageCustomAssembly"
else
  GRADLE_TASK=":polaris-custom-assembly:quarkusBuild"
fi

# ─── Summary ──────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Apache Polaris – Custom Assembly Builder"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Auth plugins     : ${AUTH_PLUGINS:-<none>}"
echo "  Federation plugins: ${FEDERATION_PLUGINS:-<none>}"
if [[ ${#MAVEN_DEPS[@]} -gt 0 ]]; then
  echo "  Extra Maven deps :"
  for dep in "${MAVEN_DEPS[@]}"; do echo "    - $dep"; done
fi
if [[ ${#EXTRA_JARS[@]} -gt 0 ]]; then
  echo "  Extra JARs       :"
  for jar in "${EXTRA_JARS[@]}"; do echo "    - $jar"; done
fi
echo "  Package          : $DO_PACKAGE"
[[ -n "$OUTPUT_DIR" ]] && echo "  Output dir       : $OUTPUT_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ─── Invoke Gradle ────────────────────────────────────────────────────────────

GRADLEW="${SCRIPT_DIR}/gradlew"
if [[ ! -x "$GRADLEW" ]]; then
  echo "ERROR: Gradle wrapper not found at ${GRADLEW}" >&2
  exit 1
fi

# shellcheck disable=SC2086
"$GRADLEW" "$GRADLE_TASK" "${GRADLE_PROPS[@]}" $EXTRA_GRADLE_ARGS

# ─── Copy output if requested ─────────────────────────────────────────────────

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  if [[ "$DO_PACKAGE" == true ]]; then
    DIST_DIR="${SCRIPT_DIR}/runtime/custom-assembly/build/distributions"
    TAR_FILE="$(find "$DIST_DIR" -name "polaris-custom-*.tar.gz" | sort | tail -1)"
    if [[ -z "$TAR_FILE" ]]; then
      echo "ERROR: No tar.gz found under $DIST_DIR" >&2
      exit 1
    fi
    cp "$TAR_FILE" "$OUTPUT_DIR/"
    echo "Copied $(basename "$TAR_FILE") → ${OUTPUT_DIR}/"
  else
    SRC="${SCRIPT_DIR}/runtime/custom-assembly/build/quarkus-app"
    cp -r "${SRC}/." "$OUTPUT_DIR/"
    echo "Copied quarkus-app → ${OUTPUT_DIR}/"
    echo "Run with: java -jar ${OUTPUT_DIR}/quarkus-run.jar"
  fi
fi
