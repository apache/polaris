/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import copiedcode.CopiedCodeCheckerExtension
import copiedcode.CopiedCodeCheckerPlugin

apply<CopiedCodeCheckerPlugin>()

extensions.getByType<CopiedCodeCheckerExtension>().apply {
  addDefaultContentTypes()
  licenseFile.convention(layout.settingsDirectory.file("LICENSE"))
}

configurations.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "com.fasterxml.woodstox" && requested.name == "woodstox-core") {
      useVersion("6.7.0")
      because("CVE-2022-40152: woodstox-core < 6.4.0 vulnerable to XML DoS; Hadoop 3.5.0 pins 5.4.0")
    }
    if (requested.group == "org.apache.commons" && requested.name == "commons-compress") {
      useVersion("1.28.0")
      because("CVE-2024-25710, CVE-2024-26308: commons-compress < 1.27 vulnerable; align with Quarkus BOM")
    }
    if (requested.group == "org.xerial.snappy" && requested.name == "snappy-java") {
      useVersion("1.1.10.12")
      because("Align snappy-java with Quarkus BOM version; Hadoop 3.5.0 pins older 1.1.10.4")
    }
    if (requested.group == "com.fasterxml.jackson.core" &&
        (requested.name == "jackson-databind" || requested.name == "jackson-core")) {
      useVersion("2.22.4")
      because(
        "Multiple CVEs (CVE-2026-54512 etc.): Hadoop 3.5.0 pins jackson 2.18.6; override to jackson-bom 2.22.4"
      )
    }
    if (requested.group == "org.apache.commons" && requested.name == "commons-configuration2") {
      useVersion("2.15.1")
      because("CVE-2026-45205: commons-configuration2 < 2.15.0 vulnerable; Hadoop 3.5.0 pins 2.10.1")
    }
    if (requested.group == "org.eclipse.jetty" || requested.group == "org.eclipse.jetty.websocket") {
      useVersion("9.4.58.v20250814")
      because("CVE-2024-13009: jetty 9.4.56 vulnerable; 9.4.58 is the latest 9.4.x release")
    }
    if (requested.group == "at.yawk.lz4" && requested.name == "lz4-java") {
      useVersion("1.12.1")
      because("CVE-2026-106449, CVE-2026-106450, CVE-2026-106451: lz4-java <= 1.11.3 vulnerable")
    }
    if (requested.group == "org.apache.zookeeper" && requested.name == "zookeeper") {
      useVersion("3.9.6")
      because(
        "Multiple CVEs (CVE-2026-59739 etc.): Hadoop 3.5.0 pins zookeeper 3.8.6; override to 3.9.6"
      )
    }
    if (
      requested.group == "io.netty" &&
        requested.name != "netty-tcnative-boringssl-static" &&
        requested.name != "netty-tcnative-classes"
    ) {
      useVersion("4.1.139.Final")
      because(
        "CVE-2026-59903 (netty-codec-http), CVE-2026-75595/CVE-2026-75596 (netty-handler):" +
          " Quarkus BOM 3.40.1 pins 4.1.138.Final which is vulnerable; 4.1.139.Final is the latest 4.1.x patch release"
      )
    }
    if (
      requested.group == "org.apache.httpcomponents.core5" &&
        (requested.name == "httpcore5" || requested.name == "httpcore5-h2")
    ) {
      useVersion("5.4.4")
      because(
        "CVE-2026-54399 (httpcore5), CVE-2026-54428 (httpcore5-h2):" +
          " 5.4 vulnerable; 5.4.4 is the latest 5.4.x maintenance release with the fix"
      )
    }
    if (requested.group == "org.bouncycastle" && requested.name == "bcprov-jdk18on") {
      useVersion("1.85")
      because(
        "CVE-2026-8763 (Critical 9.1), CVE-2026-13506 (High 7.5):" +
          " bcprov-jdk18on 1.84 vulnerable; 1.85 contains the fix"
      )
    }
    if (
      requested.group == "io.opentelemetry" && requested.name == "opentelemetry-api"
    ) {
      useVersion("1.67.0")
      because(
        "CVE-2026-45292, CVE-2026-118259: opentelemetry-api 1.60.1 vulnerable; 1.67.0 contains the fix"
      )
    }
  }
}
