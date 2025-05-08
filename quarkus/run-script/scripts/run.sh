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
#

# Linux Quarkus fast-jar run script for Apache Polaris

set -e

script_dir="$(dirname "$0")"

if [ -z "$JAVA_HOME" ] ; then
  JAVACMD="`\\unset -f command; \\command -v java`"
else
  JAVACMD="$JAVA_HOME/bin/java"
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "The JAVA_HOME environment variable is not defined correctly," >&2
  echo "this environment variable is needed to run this program." >&2
  exit 1
fi

exec "${JAVACMD}" -jar "${script_dir}/quarkus-run.jar" $@
