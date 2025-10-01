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
package org.apache.polaris.test.commons;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class OpaTestResource implements QuarkusTestResourceLifecycleManager {
  private static GenericContainer<?> opa;
  private int mappedPort;
  private Map<String, String> resourceConfig;
  private Path tempCertDir;
  private boolean useHttps;

  @Override
  public void init(Map<String, String> initArgs) {
    this.resourceConfig = initArgs;
  }

  @Override
  public Map<String, String> start() {
    // Check if HTTPS is requested
    useHttps = "true".equals(resourceConfig.get("use-https"));
    String bearerToken = resourceConfig.get("bearer-token");

    try {
      // Setup HTTPS certificates if requested
      if (useHttps) {
        setupSelfSignedCertificates();
      }

      // Reuse container across tests to speed up execution
      if (opa == null || !opa.isRunning()) {
        opa =
            new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:0.63.0"))
                .withExposedPorts(8181)
                .withReuse(true);

        if (useHttps) {
          // Configure OPA to use HTTPS with self-signed certificates
          opa.withCopyFileToContainer(
                  org.testcontainers.utility.MountableFile.forHostPath(
                      tempCertDir.resolve("server.crt")),
                  "/certs/server.crt")
              .withCopyFileToContainer(
                  org.testcontainers.utility.MountableFile.forHostPath(
                      tempCertDir.resolve("server.key")),
                  "/certs/server.key")
              .withCommand(
                  "run",
                  "--server",
                  "--addr=0.0.0.0:8181",
                  "--tls-cert-file=/certs/server.crt",
                  "--tls-private-key-file=/certs/server.key")
              .waitingFor(
                  Wait.forHttp("/health")
                      .forPort(8181)
                      .usingTls() // Use HTTPS for health check
                      .allowInsecure() // Allow self-signed certificates
                      .forStatusCode(200)
                      .withStartupTimeout(Duration.ofSeconds(120)));
        } else {
          // Configure OPA for HTTP
          opa.withCommand("run", "--server", "--addr=0.0.0.0:8181")
              .waitingFor(
                  Wait.forHttp("/health")
                      .forPort(8181)
                      .forStatusCode(200)
                      .withStartupTimeout(Duration.ofSeconds(120)));
        }

        opa.start();
      }

      mappedPort = opa.getMappedPort(8181);
      String containerHost = opa.getHost(); // This will be the actual Docker host

      String protocol = useHttps ? "https" : "http";
      String baseUrl = protocol + "://" + containerHost + ":" + mappedPort;

      // Load Rego policy into OPA
      loadRegoPolicy(baseUrl, bearerToken, "policy-name", "rego-policy");

      // Load server authentication policy only for HTTPS mode
      if (useHttps && bearerToken != null && !bearerToken.isEmpty()) {
        loadServerAuthPolicy(baseUrl, bearerToken);
      }

      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.opa.url", baseUrl);

      return config;

    } catch (Exception e) {
      throw new RuntimeException("Failed to start OPA test resource", e);
    }
  }

  private void setupSelfSignedCertificates() throws IOException {
    // Create temporary directory for certificates
    tempCertDir = Files.createTempDirectory("opa-certs");
    tempCertDir.toFile().deleteOnExit();

    Path keyFile = tempCertDir.resolve("server.key");
    Path certFile = tempCertDir.resolve("server.crt");

    try {
      // Generate self-signed certificate using Netty with BouncyCastle support
      SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");

      // Copy the generated certificate and key to our temp directory
      Files.copy(certificate.certificate().toPath(), certFile);
      Files.copy(certificate.privateKey().toPath(), keyFile);

      // Make sure files will be cleaned up
      keyFile.toFile().deleteOnExit();
      certFile.toFile().deleteOnExit();

      // Clean up the temporary Netty certificate files
      certificate.delete();
    } catch (Exception e) {
      throw new IOException("Failed to generate self-signed certificate using Netty", e);
    }
  }

  private void loadServerAuthPolicy(String baseUrl, String bearerToken) {
    // Create a server authentication policy that only allows the specific bearer token
    String serverAuthPolicy =
        String.format(
            """
        package system.authz

        default allow := false

        # Allow requests with the correct bearer token
        allow {
            input.identity.type == "bearer"
            input.identity.token == "%s"
        }

        # Allow health check endpoint without authentication
        allow {
            input.path[0] == "health"
        }
        """,
            bearerToken);

    try {
      URL url = new URL(baseUrl + "/v1/policies/server_auth");
      HttpURLConnection conn = createConnection(url);
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      // Use the bearer token to authenticate this policy upload
      conn.setRequestProperty("Authorization", "Bearer " + bearerToken);

      try (OutputStream os = conn.getOutputStream()) {
        os.write(serverAuthPolicy.getBytes(StandardCharsets.UTF_8));
      }

      int code = conn.getResponseCode();
      if (code < 200 || code >= 300) {
        throw new RuntimeException("OPA server auth policy upload failed, HTTP " + code);
      }
    } catch (Exception e) {
      String logs = "";
      try {
        logs = opa.getLogs();
      } catch (Throwable ignored) {
      }
      throw new RuntimeException(
          "Failed to load OPA server auth policy. Container logs:\n" + logs, e);
    }
  }

  private void loadRegoPolicy(
      String baseUrl, String bearerToken, String policyNameKey, String regoPolicyKey) {
    String policyName = resourceConfig.get(policyNameKey);
    String regoPolicy = resourceConfig.get(regoPolicyKey);

    if (policyName == null) {
      throw new IllegalArgumentException(
          policyNameKey + " parameter is required for OpaTestResource");
    }
    if (regoPolicy == null) {
      throw new IllegalArgumentException(
          regoPolicyKey + " parameter is required for OpaTestResource");
    }

    try {
      URL url = new URL(baseUrl + "/v1/policies/" + policyName);
      HttpURLConnection conn = createConnection(url);
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      // Add bearer token for server authentication if provided
      if (bearerToken != null && !bearerToken.isEmpty()) {
        conn.setRequestProperty("Authorization", "Bearer " + bearerToken);
      }

      try (OutputStream os = conn.getOutputStream()) {
        os.write(regoPolicy.getBytes(StandardCharsets.UTF_8));
      }

      int code = conn.getResponseCode();
      if (code < 200 || code >= 300) {
        throw new RuntimeException("OPA policy upload failed, HTTP " + code);
      }
    } catch (Exception e) {
      // Surface container logs to help debug on CI
      String logs = "";
      try {
        logs = opa.getLogs();
      } catch (Throwable ignored) {
      }
      throw new RuntimeException("Failed to load OPA policy. Container logs:\n" + logs, e);
    }
  }

  private HttpURLConnection createConnection(URL url) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    // For HTTPS connections in test environment, disable SSL verification entirely
    if (useHttps && conn instanceof HttpsURLConnection httpsConn) {
      // Create a trust-all SSL context for test purposes
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(
          null,
          new javax.net.ssl.TrustManager[] {
            new javax.net.ssl.X509TrustManager() {
              @Override
              public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
              }

              @Override
              public void checkClientTrusted(
                  java.security.cert.X509Certificate[] certs, String authType) {}

              @Override
              public void checkServerTrusted(
                  java.security.cert.X509Certificate[] certs, String authType) {}
            }
          },
          new java.security.SecureRandom());

      httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      // Disable hostname verification for test environments
      httpsConn.setHostnameVerifier((hostname, session) -> true);
    }

    return conn;
  }

  @Override
  public void stop() {
    // Don't stop the container to allow reuse across tests
    // Container will be cleaned up when the JVM exits

    // Clean up temporary certificate directory
    if (tempCertDir != null) {
      try {
        Files.walk(tempCertDir)
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(
                path -> {
                  try {
                    Files.deleteIfExists(path);
                  } catch (IOException e) {
                    // Ignore cleanup errors
                  }
                });
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }
}
