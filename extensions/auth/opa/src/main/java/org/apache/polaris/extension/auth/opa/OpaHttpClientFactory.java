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
package org.apache.polaris.extension.auth.opa;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating HTTP clients configured for OPA communication with SSL support.
 *
 * <p>This factory handles the creation of Apache HttpClient instances with proper SSL
 * configuration, timeout settings, and connection pooling for communicating with Open Policy Agent
 * (OPA) servers.
 */
class OpaHttpClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpaHttpClientFactory.class);

  /**
   * Creates a configured HTTP client for OPA communication.
   *
   * @param config HTTP configuration for timeouts and SSL settings
   * @return configured CloseableHttpClient
   */
  public static CloseableHttpClient createHttpClient(OpaAuthorizationConfig.HttpConfig config) {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setResponseTimeout(Timeout.ofMilliseconds(config.timeout().toMillis()))
            .build();

    try {
      // Create TLS strategy based on configuration
      DefaultClientTlsStrategy tlsStrategy = createTlsStrategy(config);

      // Create connection manager with the TLS strategy
      var connectionManager =
          PoolingHttpClientConnectionManagerBuilder.create()
              .setTlsSocketStrategy(tlsStrategy)
              .build();

      return HttpClients.custom()
          .setConnectionManager(connectionManager)
          .setDefaultRequestConfig(requestConfig)
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create HTTP client for OPA communication", e);
    }
  }

  /**
   * Creates a TLS strategy based on the configuration.
   *
   * @param config HTTP configuration containing SSL settings
   * @return DefaultClientTlsStrategy for HTTPS connections
   */
  private static DefaultClientTlsStrategy createTlsStrategy(
      OpaAuthorizationConfig.HttpConfig config) throws Exception {
    SSLContext sslContext = createSslContext(config);

    if (!config.verifySsl()) {
      // Disable hostname verification when SSL verification is disabled
      return new DefaultClientTlsStrategy(sslContext, NoopHostnameVerifier.INSTANCE);
    } else {
      // Use default hostname verification when SSL verification is enabled
      return new DefaultClientTlsStrategy(sslContext);
    }
  }

  /**
   * Creates an SSL context based on the configuration.
   *
   * @param config HTTP configuration containing SSL settings
   * @return SSLContext for HTTPS connections
   */
  private static SSLContext createSslContext(OpaAuthorizationConfig.HttpConfig config)
      throws Exception {
    if (!config.verifySsl()) {
      // Disable SSL verification (for development/testing)
      LOGGER.warn(
          "SSL verification is disabled for OPA server. This should only be used in development/testing environments.");
      return SSLContexts.custom()
          .loadTrustMaterial(
              null, (X509Certificate[] chain, String authType) -> true) // trust all certificates
          .build();
    } else if (config.trustStorePath().isPresent()) {
      // Load custom trust store for SSL verification
      Path trustStorePath = config.trustStorePath().get();
      LOGGER.info("Loading custom trust store for OPA SSL verification: {}", trustStorePath);
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (FileInputStream trustStoreStream = new FileInputStream(trustStorePath.toFile())) {
        String trustStorePassword = config.trustStorePassword().orElse(null);
        trustStore.load(
            trustStoreStream, trustStorePassword != null ? trustStorePassword.toCharArray() : null);
      }
      return SSLContexts.custom().loadTrustMaterial(trustStore, null).build();
    } else {
      // Use default system trust store for SSL verification
      LOGGER.debug("Using default system trust store for OPA SSL verification");
      return SSLContexts.createDefault();
    }
  }
}
