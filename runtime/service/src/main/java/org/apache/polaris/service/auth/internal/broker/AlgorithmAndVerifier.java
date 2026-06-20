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
import com.auth0.jwt.interfaces.JWTVerifier;

/**
 * Holds a JWT {@link Algorithm} and its derived {@link JWTVerifier}. Built once per realm by a
 * {@link TokenBrokerFactory} and shared across request-scoped {@link JWTBroker} instances so the
 * verifier is not reconstructed on every request.
 */
record AlgorithmAndVerifier(Algorithm algorithm, JWTVerifier verifier) {

  static AlgorithmAndVerifier of(Algorithm algorithm) {
    return new AlgorithmAndVerifier(algorithm, JWTBroker.buildVerifier(algorithm));
  }
}
