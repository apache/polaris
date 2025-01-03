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
package org.apache.polaris.service.auth;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that can load public / private keys stored on localhost. Meant to be a simple
 * implementation for now where a PEM file is loaded off disk.
 */
public class LocalRSAKeyProvider implements KeyProvider {

  private static final String LOCAL_PRIVATE_KEY_LOCATION_KEY = "LOCAL_PRIVATE_KEY_LOCATION_KEY";
  private static final String LOCAL_PUBLIC_KEY_LOCATION_KEY = "LOCAL_PUBLIC_LOCATION_KEY";

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalRSAKeyProvider.class);

  private String getLocation(String configKey) {
    CallContext callContext = CallContext.getCurrentContext();
    PolarisCallContext pCtx = callContext.getPolarisCallContext();
    String fileLocation = pCtx.getConfigurationStore().getConfiguration(pCtx, configKey);
    if (fileLocation == null) {
      throw new RuntimeException("Cannot find location for key " + configKey);
    }
    return fileLocation;
  }

  /**
   * Getter for the Public Key instance
   *
   * @return the Public Key instance
   */
  @Override
  public PublicKey getPublicKey() {
    final String publicKeyFileLocation = getLocation(LOCAL_PUBLIC_KEY_LOCATION_KEY);
    try {
      return PemUtils.readPublicKeyFromFile(publicKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read public key from file {}", publicKeyFileLocation, e);
      throw new RuntimeException("Unable to read public key from file " + publicKeyFileLocation, e);
    }
  }

  /**
   * Getter for the Private Key instance. Used to sign the content on the JWT signing stage.
   *
   * @return the Private Key instance
   */
  @Override
  public PrivateKey getPrivateKey() {
    final String privateKeyFileLocation = getLocation(LOCAL_PRIVATE_KEY_LOCATION_KEY);
    try {
      return PemUtils.readPrivateKeyFromFile(privateKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read private key from file {}", privateKeyFileLocation, e);
      throw new RuntimeException(
          "Unable to read private key from file " + privateKeyFileLocation, e);
    }
  }
}
