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

import org.apache.polaris.service.auth.PolarisCredential;
import org.jspecify.annotations.Nullable;

/**
 * Outcome of {@link TokenBroker#verify(String)}.
 *
 * <p>Modeling the normal outcomes as data, rather than a {@code null} return combined with
 * exceptions, lets the authentication mechanism distinguish a token that is not recognized by this
 * broker from one that is recognized but invalid without inspecting exception types. Only genuinely
 * transient or unexpected failures are surfaced as exceptions from {@code verify}.
 */
public sealed interface TokenVerificationResult
    permits TokenVerificationResult.Recognized,
        TokenVerificationResult.NotRecognized,
        TokenVerificationResult.Invalid {

  /** The token was recognized by this broker and successfully verified. */
  record Recognized(PolarisCredential credential) implements TokenVerificationResult {}

  /**
   * The token is not recognized by this broker (for example it does not claim to be Polaris-issued,
   * or it cannot be decoded). In MIXED mode the mechanism may delegate to other authentication
   * mechanisms; otherwise authentication fails.
   */
  record NotRecognized() implements TokenVerificationResult {}

  /**
   * The token is recognized as Polaris-issued but failed verification (bad signature, missing or
   * invalid claims, and so on). This is a definitive authentication failure and does not enable
   * MIXED fallback.
   */
  record Invalid(String message, @Nullable Throwable cause) implements TokenVerificationResult {}
}
