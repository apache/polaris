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

plugins {
  id("base")
  id("polaris-spotless")
}

description = "Apache Polaris (incubating) Helm Chart"

val runtimeServerDistribution by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

dependencies { runtimeServerDistribution(project(":polaris-server", "distributionElements")) }

val helmTestReportsDir = layout.buildDirectory.dir("reports")

val helmTemplateValidation by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run 'helm template' validation with all values files"

    val outputFile = helmTestReportsDir.get().file("helm-template/validation.log").asFile

    doFirst {
      outputFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          set -e
          for f in values.yaml ci/*.yaml; do
            echo "Validating helm template with ${'$'}f"
            helm template --debug --namespace polaris-ns --values ${'$'}f .
          done
        """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/*.yaml", "templates/**/*") }
    )

    outputs.cacheIf { true }
  }

val helmUnitTest by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run Helm unit tests"

    val outputFile = helmTestReportsDir.get().file("helm-unit/test.log").asFile

    doFirst {
      outputFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          set -e
          echo "====== Install helm-unittest plugin ======"
          helm plugin install https://github.com/helm-unittest/helm-unittest.git || true

          echo "====== Run helm unit tests ======"
          helm unittest .
        """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "templates/**/*", "tests/**/*") }
    )

    outputs.cacheIf { true }
  }

val chartTestingLint by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run chart-testing (lint)"

    val outputFile = helmTestReportsDir.get().file("ct/lint.log").asFile

    doFirst {
      outputFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          set -e
          ct lint --debug --charts .
        """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/*.yaml", "templates/**/*") }
    )

    outputs.cacheIf { true }
  }

// Task to build Docker images in minikube's Docker environment
val buildMinikubeImages by
  tasks.registering(Exec::class) {
    group = "build"
    description = "Build Polaris Docker images in minikube's Docker environment"

    // Must be run from root project directory because Dockerfile.jvm must be
    // contained within the build context
    workingDir = rootProject.projectDir

    val outputFile = helmTestReportsDir.get().file("minikube-images/build.log").asFile
    val digestFile = helmTestReportsDir.get().file("minikube-images/build.sha256").asFile
    val dockerFile = project(":polaris-server").projectDir.resolve("src/main/docker/Dockerfile.jvm")

    doFirst {
      outputFile.parentFile.mkdirs()
      digestFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
    set -e
    echo "====== Check if minikube is running ======"
    if ! minikube status >/dev/null 2>&1; then
      echo "Minikube is not running. Starting minikube..."
      minikube start
    fi

    echo "====== Set up docker environment and build images ======"
    eval $(minikube -p minikube docker-env)

    echo "====== Build server image ======"
    docker build -t apache/polaris:latest \
      -f ${dockerFile.relativeTo(workingDir)} \
      ${project(":polaris-server").projectDir.relativeTo(workingDir)}

    {
      docker inspect --format='{{.Id}}' apache/polaris:latest
    } > ${digestFile.relativeTo(workingDir)}
  """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    dependsOn(":polaris-server:quarkusBuild")

    inputs.files(runtimeServerDistribution)
    inputs.file(dockerFile)

    outputs.cacheIf { true }
  }

// Task to run chart-testing install on minikube
val chartTestingInstall by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run chart-testing (install) on minikube"

    val outputFile = helmTestReportsDir.get().file("ct/install.log").asFile

    doFirst {
      outputFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          set -eo pipefail
          echo "====== Check if minikube is running ======"
          if ! minikube status >/dev/null 2>&1; then
            echo "Minikube is not running. Starting minikube..."
            minikube start
          fi

          echo "====== Create namespace if it doesn't exist ======"
          kubectl create namespace polaris-ns --dry-run=client -o yaml | kubectl apply -f -

          echo "===== Install fixtures ======"
          kubectl apply --namespace polaris-ns -f helm/polaris/ci/fixtures

          echo "===== Run chart-testing install ======"
          ct install --namespace polaris-ns --debug --charts .
        """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    dependsOn(buildMinikubeImages)

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/**/*", "templates/**/*") }
    )

    outputs.cacheIf { true }
  }

val helmDocs by
  tasks.registering(Exec::class) {
    group = "documentation"
    description = "Generate Helm chart documentation using helm-docs"

    val outputFile = helmTestReportsDir.get().file("helm-docs/build.log").asFile

    doFirst {
      outputFile.parentFile.mkdirs()
      val outStream = outputFile.outputStream()
      standardOutput = outStream
      errorOutput = outStream
    }

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          set -e
          helm-docs --chart-search-root=.
        """
          .trimIndent(),
      )

    checkExitCode(outputFile)

    inputs.files(fileTree(projectDir) { include("Chart.yaml", "values.yaml", "README.md.gotmpl") })

    outputs.cacheIf { true }
  }

val test by
  tasks.registering {
    group = "verification"
    description = "Run all Helm chart tests"
    dependsOn(helmTemplateValidation, helmUnitTest, chartTestingLint)
  }

val intTest by
  tasks.registering {
    group = "verification"
    description = "Run Helm chart integration tests on minikube"
    dependsOn(chartTestingInstall)
    mustRunAfter(test)
  }

tasks.named("check") { dependsOn(test, intTest) }

tasks.named("assemble") { dependsOn(helmDocs) }

fun Exec.checkExitCode(outputFile: File) {
  isIgnoreExitValue = true
  doLast {
    val exitValue = executionResult.get().exitValue
    if (exitValue != 0) {
      logger.error("Shell script failed with exit code $exitValue.")
      logger.error("To identify the cause of the failure, inspect the logs:")
      logger.error(outputFile.absolutePath)
      throw GradleException("Shell script failed with exit code $exitValue")
    }
  }
}
