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

import org.gradle.api.Project
import org.gradle.api.tasks.TaskProvider
import org.gradle.internal.extensions.stdlib.capitalized
import org.gradle.plugins.signing.SigningExtension

fun Project.isSigningEnabled(): Boolean = hasProperty("release") || hasProperty("signArtifacts")

/**
 * Convenience function to sign all output files of the given task.
 *
 * Only triggers signing, if the `release` or `signArtifacts` project property is set.
 */
fun Project.signTaskOutputs(task: TaskProvider<*>): Unit {
  if (isSigningEnabled()) {
    val signingTask = tasks.register("sign" + task.name.capitalized())
    signingTask.configure {
      dependsOn(task)
      actions.addLast {
        val files = task.get().outputs.files.files
        files.forEach { file -> logger.info("Signing $file for '${task.get().path}'") }
        val ext = project.extensions.getByType(SigningExtension::class.java)
        ext.sign(*files.toTypedArray())
      }
    }
    task.configure { finalizedBy(signingTask) }
  }
}
