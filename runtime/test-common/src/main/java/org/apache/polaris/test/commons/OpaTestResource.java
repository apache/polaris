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

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.testcontainers.containers.BindMode;
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
          opa.withFileSystemBind(tempCertDir.toString(), "/certs", BindMode.READ_ONLY)
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

      String protocol = useHttps ? "https" : "http";
      String baseUrl = protocol + "://localhost:" + mappedPort;

      // Load Rego policy into OPA
      loadRegoPolicy(baseUrl, bearerToken, "policy-name", "rego-policy");

      // Load server authentication policy only for HTTPS mode
      if (useHttps && bearerToken != null && !bearerToken.isEmpty()) {
        loadServerAuthPolicy(baseUrl, bearerToken);
      }

      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.opa.url", baseUrl);

      // For HTTPS mode, provide the trust store path for SSL verification
      if (useHttps && tempCertDir != null) {
        config.put(
            "polaris.authorization.opa.trust-store-path",
            tempCertDir.resolve("truststore.jks").toString());
        config.put("polaris.authorization.opa.trust-store-password", "test-password");
      }

      return config;

    } catch (Exception e) {
      throw new RuntimeException("Failed to start OPA test resource", e);
    }
  }

  private void setupSelfSignedCertificates() throws IOException, InterruptedException {
    // Create temporary directory for certificates
    tempCertDir = Files.createTempDirectory("opa-certs");
    tempCertDir.toFile().deleteOnExit();

    Path keyFile = tempCertDir.resolve("server.key");
    Path certFile = tempCertDir.resolve("server.crt");
    Path trustStoreFile = tempCertDir.resolve("truststore.jks");
    Path configFile = tempCertDir.resolve("openssl.conf");

    // Create OpenSSL config for self-signed certificate with SAN
    String opensslConfig =
        """
        [req]
        default_bits = 2048
        prompt = no
        default_md = sha256
        distinguished_name = dn
        req_extensions = v3_req

        [dn]
        C=US
        ST=Test
        L=Test
        O=Test
        CN=localhost

        [v3_req]
        subjectAltName = @alt_names

        [alt_names]
        DNS.1 = localhost
        DNS.2 = opa
        IP.1 = 127.0.0.1
        """;

    try (FileWriter writer = new FileWriter(configFile.toFile(), StandardCharsets.UTF_8)) {
      writer.write(opensslConfig);
    }

    // Generate private key
    ProcessBuilder keyGenProcess =
        new ProcessBuilder("openssl", "genrsa", "-out", keyFile.toString(), "2048");
    Process keyGen = keyGenProcess.start();
    if (keyGen.waitFor() != 0) {
      throw new RuntimeException("Failed to generate private key for OPA HTTPS");
    }

    // Generate self-signed certificate
    ProcessBuilder certGenProcess =
        new ProcessBuilder(
            "openssl",
            "req",
            "-new",
            "-x509",
            "-key",
            keyFile.toString(),
            "-out",
            certFile.toString(),
            "-days",
            "365",
            "-config",
            configFile.toString(),
            "-extensions",
            "v3_req");
    Process certGen = certGenProcess.start();
    if (certGen.waitFor() != 0) {
      throw new RuntimeException("Failed to generate certificate for OPA HTTPS");
    }

    // Create a trust store containing the self-signed certificate
    createTrustStore(certFile, trustStoreFile);

    // Make sure files will be cleaned up
    keyFile.toFile().deleteOnExit();
    certFile.toFile().deleteOnExit();
    trustStoreFile.toFile().deleteOnExit();
    configFile.toFile().deleteOnExit();
  }

  private void createTrustStore(Path certFile, Path trustStoreFile) throws IOException {
    try {
      // Load the certificate
      CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
      X509Certificate certificate;
      try (FileInputStream certInputStream = new FileInputStream(certFile.toFile())) {
        certificate = (X509Certificate) certificateFactory.generateCertificate(certInputStream);
      }

      // Create a new trust store
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null); // Initialize empty trust store

      // Add the certificate to the trust store
      trustStore.setCertificateEntry("opa-test-cert", certificate);

      // Save the trust store to file
      try (var trustStoreOutputStream = Files.newOutputStream(trustStoreFile)) {
        trustStore.store(trustStoreOutputStream, "test-password".toCharArray());
      }
    } catch (Exception e) {
      throw new IOException("Failed to create trust store", e);
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

    // Configure SSL context to use the same trust store as OpaPolarisAuthorizer for HTTPS
    if (useHttps && conn instanceof HttpsURLConnection httpsConn) {
      // Load the trust store we created for consistent SSL verification
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      Path trustStoreFile = tempCertDir.resolve("truststore.jks");
      try (FileInputStream trustStoreStream = new FileInputStream(trustStoreFile.toFile())) {
        trustStore.load(trustStoreStream, "test-password".toCharArray());
      }

      // Create SSL context with the trust store
      SSLContext sslContext = SSLContext.getInstance("TLS");
      javax.net.ssl.TrustManagerFactory tmf =
          javax.net.ssl.TrustManagerFactory.getInstance(
              javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      sslContext.init(null, tmf.getTrustManagers(), new java.security.SecureRandom());

      httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      // We can verify hostname since our certificate includes localhost
      httpsConn.setHostnameVerifier(
          (hostname, session) -> "localhost".equals(hostname) || "127.0.0.1".equals(hostname));
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
