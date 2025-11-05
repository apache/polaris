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

private val missingDependencyExitCode = -42

val helmChecks =
  """
        echo "====== Check if helm is installed ======"
        if ! command -v helm >/dev/null 2>&1; then
          echo "helm is not installed."
          # Check if we're on macOS
          if [[ "${'$'}(uname -s)" == "Darwin" ]]; then
            # Check if brew is available
            if command -v brew >/dev/null 2>&1; then
              echo "Installing helm using Homebrew..."
              brew install helm
            else
              echo "WARNING: Homebrew is not installed. Cannot auto-install helm."
              echo "Please install Homebrew from https://brew.sh/ or install helm manually."
              exit $missingDependencyExitCode
            fi
          else
            echo "WARNING: helm is not installed."
            echo "To install helm, see https://helm.sh/docs/intro/install/"
            exit $missingDependencyExitCode
          fi
        fi
    """

val chartTestingChecks =
  """
    echo "====== Check if chart-testing is installed ======"
    if ! command -v ct >/dev/null 2>&1; then
      echo "chart-testing is not installed."
      # Check if we're on macOS
      if [[ "${'$'}(uname -s)" == "Darwin" ]]; then
        # Check if brew is available
        if command -v brew >/dev/null 2>&1; then
          echo "Installing chart-testing using Homebrew..."
          brew install chart-testing
        else
          echo "WARNING: Homebrew is not installed. Cannot auto-install chart-testing."
          echo "Please install Homebrew from https://brew.sh/ or install chart-testing manually."
          exit $missingDependencyExitCode
        fi
      else
        echo "WARNING: chart-testing is not installed."
        echo "To install chart-testing, see https://github.com/helm/chart-testing."
        exit $missingDependencyExitCode
      fi
    fi
    """

val minikubeChecks =
  """ 
    echo "====== Check if Minikube is installed ======"
    if ! command -v minikube >/dev/null 2>&1; then
      echo "Minikube is not installed."
      echo "To install minikube, see https://minikube.sigs.k8s.io/docs/start/."
      exit $missingDependencyExitCode
    fi

    echo "====== Check if Minikube is running ======"
    if ! minikube status >/dev/null 2>&1; then
      echo "Minikube is not running. Starting minikube..."
      minikube start
    fi
   """

val kubectlChecks =
  """
    echo "====== Check if kubectl is installed ======"
    if ! command -v kubectl >/dev/null 2>&1; then
      echo "kubectl is not installed."
      echo "To install kubectl, see https://kubernetes.io/docs/tasks/tools/"
      exit $missingDependencyExitCode
    fi
  """

val helmDocsChecks =
  """
    echo "====== Check if helm-docs is installed ======"
      if ! command -v helm-docs >/dev/null 2>&1; then
        echo "helm-docs is not installed."

        # Check if we're on macOS
        if [[ "${'$'}(uname -s)" == "Darwin" ]]; then
          # Check if brew is available
          if command -v brew >/dev/null 2>&1; then
            echo "Installing helm-docs using Homebrew..."
            brew install norwoodj/tap/helm-docs
          else
            echo "WARNING: Homebrew is not installed. Cannot auto-install helm-docs."
            echo "Please install Homebrew from https://brew.sh/ or install helm-docs manually."
            exit $missingDependencyExitCode
          fi
        else
          echo "WARNING: helm-docs is not installed. Skipping documentation generation."
          echo "To install helm-docs on Linux, download from: https://github.com/norwoodj/helm-docs/releases"
          exit $missingDependencyExitCode
        fi
      fi
      """

val helmTemplateValidation by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run 'helm template' validation with all values files"

    val outputFile = helmTestReportsDir.get().file("helm-template/validation.log").asFile

    runShellScript(
      """
          set -e
          ${helmChecks.trimIndent()}
          for f in values.yaml ci/*.yaml; do
            echo "Validating helm template with ${'$'}f"
            helm template --debug --namespace polaris-ns --values ${'$'}f .
          done
        """,
      outputFile,
    )

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

    runShellScript(
      """
          set -e
          ${helmChecks.trimIndent()}
          echo "====== Install helm-unittest plugin ======"
          helm plugin install https://github.com/helm-unittest/helm-unittest.git || true

          echo "====== Run helm unit tests ======"
          helm unittest .
        """,
      outputFile,
    )

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

    runShellScript(
      """
          set -e
          ${helmChecks.trimIndent()}
          ${chartTestingChecks.trimIndent()}
          ct lint --debug --charts .
        """,
      outputFile,
    )

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

    digestFile.parentFile.mkdirs()

    runShellScript(
      """
    set -e

    ${minikubeChecks.trimIndent()}

    echo "====== Set up docker environment and build images ======"
    eval $(minikube -p minikube docker-env)

    echo "====== Build server image ======"
    docker build -t apache/polaris:latest \
      -f ${dockerFile.relativeTo(workingDir)} \
      ${project(":polaris-server").projectDir.relativeTo(workingDir)}

    {
      docker inspect --format='{{.Id}}' apache/polaris:latest
    } > ${digestFile.relativeTo(workingDir)}
  """,
      outputFile,
    )

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

    runShellScript(
      """
          set -eo pipefail
          
          ${helmChecks.trimIndent()}
          ${minikubeChecks.trimIndent()}
          ${chartTestingChecks.trimIndent()}
          ${kubectlChecks.trimIndent()}

          echo "====== Create namespace if it doesn't exist ======"
          kubectl create namespace polaris-ns --dry-run=client -o yaml | kubectl apply -f -

          echo "===== Install fixtures ======"
          kubectl apply --namespace polaris-ns -f ci/fixtures

          echo "===== Run chart-testing install ======"
          ct install --namespace polaris-ns --debug --charts .
        """,
      outputFile,
    )

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

    runShellScript(
      """
          set -e

          ${helmChecks.trimIndent()}
          ${helmDocsChecks.trimIndent()}

          echo "====== Generate Helm documentation ======"
          helm-docs --chart-search-root=.
        """,
      outputFile,
    )

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

fun Exec.runShellScript(script: String, outputFile: File) {
  doFirst {
    outputFile.parentFile.mkdirs()
    val outStream = outputFile.outputStream()
    standardOutput = outStream
    errorOutput = outStream
  }
  commandLine = listOf("bash", "-c", script.trimIndent())
  this.isIgnoreExitValue = true
  doLast {
    val exitValue = executionResult.get().exitValue
    if (exitValue != 0) {
      if (exitValue == missingDependencyExitCode) {
        this.logger.warn("Missing required executable, skipping task:")
        outputFile.readLines().forEach { this.logger.warn(it) }
      } else {
        this.logger.error("Shell script failed with exit code $exitValue:\n")
        outputFile.readLines().forEach { this.logger.error(it) }
        throw GradleException("Shell script failed with exit code $exitValue")
      }
    }
  }
}
