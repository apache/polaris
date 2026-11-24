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
    if (
      (requested.group == "org.eclipse.jetty" || requested.group == "org.eclipse.jetty.websocket") &&
        requested.version?.startsWith("9.") == true
    ) {
      useVersion("9.4.58.v20250814")
      because(
        "Build-wide Jetty 9.x pin: align transitive Jetty 9.x dependencies (e.g. from hive-metastore, ranger)" +
          " to 9.4.58.v20250814 to address CVE-2024-13009",
      )
    }
  }
}
