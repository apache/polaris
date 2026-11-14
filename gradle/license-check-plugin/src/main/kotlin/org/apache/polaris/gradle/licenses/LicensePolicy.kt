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

package org.apache.polaris.gradle.licenses

import groovy.json.JsonException
import groovy.json.JsonSlurper
import java.io.File
import org.gradle.api.GradleException

/** Validated policy values, independent of Gradle configuration and Licensee application. */
internal class LicensePolicy(contents: String, source: File) {
  val allowedLicenses: List<String>
  val allowedUrls: List<Map<String, String>>
  val allowedDependencies: List<Map<String, String>>

  init {
    val parsed =
      try {
        JsonSlurper().parseText(contents)
      } catch (e: JsonException) {
        throw GradleException("Invalid license policy JSON: $source", e)
      }
    val policy =
      parsed as? Map<*, *> ?: throw GradleException("License policy must be a JSON object: $source")
    require(
      policy.keys.all {
        it in setOf("_comment", "allowedLicenses", "allowedUrls", "allowedDependencies")
      },
    ) {
      "Unknown license policy field in $source: ${policy.keys}"
    }
    if (policy.containsKey("_comment")) {
      val comment = policy["_comment"]
      require(comment is String || (comment is List<*> && comment.all { it is String })) {
        "_comment must be a string or array of strings"
      }
    }
    allowedLicenses =
      entries(policy, "allowedLicenses").map { value ->
        require(value is String && value.isNotBlank()) {
          "allowedLicenses entries must be nonempty SPDX identifiers"
        }
        value
      }
    allowedUrls =
      entries(policy, "allowedUrls").map { entry(it, setOf("url", "reason"), "allowedUrls") }
    allowedDependencies =
      entries(policy, "allowedDependencies").map {
        entry(it, setOf("group", "artifact", "version", "reason"), "allowedDependencies")
      }
  }

  private fun entries(policy: Map<*, *>, key: String): List<*> {
    if (!policy.containsKey(key)) return emptyList<Any>()
    return policy[key] as? List<*> ?: throw GradleException("$key must be an array")
  }

  private fun entry(value: Any?, keys: Set<String>, section: String): Map<String, String> {
    require(value is Map<*, *> && value.keys == keys) {
      "$section entries must contain exactly $keys"
    }
    return keys.associateWith { key ->
      val text = value[key]
      require(text is String && text.isNotBlank()) { "$section.$key must be a nonempty string" }
      text
    }
  }
}
