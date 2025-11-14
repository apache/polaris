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
package org.apache.polaris.service.auth.internal.broker;

import com.auth0.jwt.algorithms.Algorithm;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;

/** Generates a JWT using a Public/Private RSA Key */
public class RSAKeyPairJWTBroker extends JWTBroker {

  private final KeyProvider keyProvider;

  RSAKeyPairJWTBroker(
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      int maxTokenGenerationInSeconds,
      KeyProvider keyProvider) {
    super(metaStoreManager, polarisCallContext, maxTokenGenerationInSeconds);
    this.keyProvider = keyProvider;
  }

  @Override
  public Algorithm getAlgorithm() {
    return Algorithm.RSA256(
        (RSAPublicKey) keyProvider.publicKey(), (RSAPrivateKey) keyProvider.privateKey());
  }
}
