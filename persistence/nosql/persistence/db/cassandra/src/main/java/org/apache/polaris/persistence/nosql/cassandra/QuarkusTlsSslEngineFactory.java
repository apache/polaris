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
package org.apache.polaris.persistence.nosql.cassandra;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import io.vertx.core.net.SSLOptions;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/** Applies the TLS Registry's protocol and cipher-suite settings to driver TLS engines. */
final class QuarkusTlsSslEngineFactory extends ProgrammaticSslEngineFactory {
  private final Set<String> enabledCipherSuites;
  private final Set<String> enabledProtocols;

  QuarkusTlsSslEngineFactory(
      SSLContext sslContext, SSLOptions sslOptions, boolean hostnameValidation) {
    super(sslContext, null, hostnameValidation);
    enabledCipherSuites = sslOptions.getEnabledCipherSuites();
    enabledProtocols = sslOptions.getEnabledSecureTransportProtocols();
  }

  @Override
  public SSLEngine newSslEngine(EndPoint remoteEndpoint) {
    var engine = super.newSslEngine(remoteEndpoint);
    if (!enabledProtocols.isEmpty()) {
      engine.setEnabledProtocols(enabledProtocols.toArray(String[]::new));
    }
    if (!enabledCipherSuites.isEmpty()) {
      engine.setEnabledCipherSuites(enabledCipherSuites.toArray(String[]::new));
    }
    return engine;
  }
}
