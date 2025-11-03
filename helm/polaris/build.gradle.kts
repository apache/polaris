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

val helmTestReportsDir = layout.buildDirectory.dir("reports")

val helmTemplateValidation by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run 'helm template' validation with all values files"

    workingDir = projectDir

    val outputFile = helmTestReportsDir.get().file("helm-template/validation.log")

    commandLine =
      listOf(
        "sh",
        "-c",
        """
    mkdir -p ${outputFile.asFile.parent}
    for f in values.yaml ci/*.yaml; do
      echo "Validating helm template with ${'$'}f"
      helm template --debug --namespace polaris-ns --values ${'$'}f .
    done > ${outputFile.asFile} 2>&1
  """
          .trimIndent(),
      )

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/*.yaml", "templates/**/*") }
    )

    outputs.file(outputFile)
  }

val helmUnitTest by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run Helm unit tests"

    workingDir = projectDir

    val outputFile = helmTestReportsDir.get().file("helm-unit/test.log")

    // Install the plugin if not already installed, then run tests
    commandLine =
      listOf(
        "sh",
        "-c",
        """
    mkdir -p ${outputFile.asFile.parent}
    helm plugin install https://github.com/helm-unittest/helm-unittest.git 2>/dev/null || true
    helm unittest . > ${outputFile.asFile} 2>&1
  """
          .trimIndent(),
      )

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "templates/**/*", "tests/**/*") }
    )

    outputs.file(outputFile)
  }

val chartTestingLint by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run chart-testing (lint)"

    workingDir = rootProject.projectDir

    val outputFile = helmTestReportsDir.get().file("ct/lint.log")

    commandLine =
      listOf(
        "sh",
        "-c",
        """
    mkdir -p ${outputFile.asFile.parent}
    ct lint --debug --charts ./helm/polaris > ${outputFile.asFile} 2>&1
  """
          .trimIndent(),
      )

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/*.yaml", "templates/**/*") }
    )

    outputs.file(outputFile)
  }

// Task to build Docker images in minikube's Docker environment
val buildMinikubeImages by
  tasks.registering(Exec::class) {
    group = "build"
    description = "Build Polaris Docker images in minikube's Docker environment"

    workingDir = rootProject.projectDir

    // Output: file containing SHA digest of built image
    val outputFile = helmTestReportsDir.get().file("minikube-images.sha256")

    commandLine =
      listOf(
        "sh",
        "-c",
        """
    # Check if minikube is running
    if ! minikube status >/dev/null 2>&1; then
      echo "Minikube is not running. Starting minikube..."
      minikube start
    fi

    # Set up docker environment and build images
    eval $(minikube -p minikube docker-env)

    # Build server image
    docker build -t apache/polaris:latest \
      -f runtime/server/src/main/docker/Dockerfile.jvm \
      runtime/server

    # Capture image digest
    mkdir -p ${outputFile.asFile.parent}
    {
      docker inspect --format='{{.Id}}' apache/polaris:latest
    } > ${outputFile.asFile}
  """
          .trimIndent(),
      )

    dependsOn(":polaris-server:quarkusBuild")

    inputs.dir(rootProject.file("runtime/server/build/quarkus-app"))
    inputs.file(rootProject.file("runtime/server/src/main/docker/Dockerfile.jvm"))

    outputs.file(outputFile)
  }

// Task to run chart-testing install on minikube
val chartTestingInstall by
  tasks.registering(Exec::class) {
    group = "verification"
    description = "Run chart-testing (install) on minikube"

    workingDir = rootProject.projectDir

    val outputFile = helmTestReportsDir.get().file("ct/install.log")

    commandLine =
      listOf(
        "sh",
        "-c",
        """
    mkdir -p ${outputFile.asFile.parent}

    # Create namespace if it doesn't exist
    kubectl create namespace polaris-ns --dry-run=client -o yaml | kubectl apply -f -

    # Install fixtures
    kubectl apply --namespace polaris-ns -f helm/polaris/ci/fixtures

    # Run chart-testing install
    ct install \
      --namespace polaris-ns \
      --debug --charts ./helm/polaris > ${outputFile.asFile} 2>&1
  """
          .trimIndent(),
      )

    inputs.files(
      fileTree(projectDir) { include("Chart.yaml", "values.yaml", "ci/**/*", "templates/**/*") }
    )

    outputs.file(outputFile)

    // Depend on the images being built in minikube
    dependsOn(buildMinikubeImages)
  }

// Task to generate helm documentation
val helmDocs by
  tasks.registering(Exec::class) {
    group = "documentation"
    description = "Generate Helm chart documentation using helm-docs"

    workingDir = rootProject.projectDir

    commandLine =
      listOf(
        "sh",
        "-c",
        """
          helm-docs --chart-search-root=helm
        """
          .trimIndent(),
      )

    inputs.files(fileTree(projectDir) { include("Chart.yaml", "values.yaml", "README.md.gotmpl") })

    outputs.file(projectDir.resolve("README.md"))
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
  }

tasks.named("check") { dependsOn(test) }

tasks.named("build") {
  dependsOn(intTest)
  dependsOn(helmDocs)
}
