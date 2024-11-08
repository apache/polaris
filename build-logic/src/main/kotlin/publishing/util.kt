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

package publishing

import groovy.json.JsonException
import groovy.json.JsonSlurper
import groovy.util.Node
import groovy.util.NodeList
import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.security.MessageDigest
import org.gradle.api.GradleException
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.artifacts.result.DependencyResult

internal fun findDependency(
  config: Configuration?,
  depGroup: String,
  depName: String
): DependencyResult? {
  if (config != null) {
    val depResult =
      config.incoming.resolutionResult.allDependencies.find { depResult ->
        val req = depResult.requested
        if (req is ModuleComponentSelector) req.group == depGroup && req.module == depName
        else false
      }
    return depResult
  }
  return null
}

internal fun xmlNode(node: Node?, child: String): Node? {
  val found = node?.get(child)
  if (found is NodeList) {
    if (found.isNotEmpty()) {
      return found[0] as Node
    }
  }
  return null
}

internal fun generateDigest(input: File, output: File, algorithm: String) {
  val md = MessageDigest.getInstance(algorithm)
  input.inputStream().use {
    val buffered = it.buffered(8192)
    val buf = ByteArray(8192)
    var rd: Int
    while (true) {
      rd = buffered.read(buf)
      if (rd == -1) break
      md.update(buf, 0, rd)
    }

    output.writeText(
      md.digest().joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
    )
  }
}

internal fun fetchAsfProject(asfName: String): Pair<Map<String, Any>, Boolean> {
  val tlpPrj: Map<String, Any>? =
    parseJson("https://projects.apache.org/json/projects/$asfName.json")
  return if (tlpPrj != null) {
    Pair(tlpPrj, false)
  } else {
    val podlings: Map<String, Map<String, Any>> =
      parseJson("https://projects.apache.org/json/foundation/podlings.json")!!
    val podling = podlings[asfName]
    if (podling == null) {
      throw GradleException("Neither a project nor a podling for $asfName could be found at Apache")
    }
    Pair(podling, true)
  }
}

internal fun <T : Any> unsafeCast(o: Any?): T {
  @Suppress("UNCHECKED_CAST") return o as T
}

internal fun <T : Any> parseJson(url: String): T? {
  try {
    @Suppress("UNCHECKED_CAST") return JsonSlurper().parse(URI(url).toURL()) as T
  } catch (e: JsonException) {
    if (e.cause is FileNotFoundException) {
      return null
    }
    throw e
  }
}
