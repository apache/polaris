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

import groovy.util.Node
import groovy.util.NodeList
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.artifacts.result.DependencyResult

internal fun findDependency(
  config: Configuration?,
  depGroup: String,
  depName: String,
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
