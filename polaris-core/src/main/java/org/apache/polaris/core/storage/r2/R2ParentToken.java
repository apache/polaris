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
package org.apache.polaris.core.storage.r2;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.jspecify.annotations.NonNull;

/**
 * An R2 API token that Polaris holds server-side and uses to mint temporary credentials. The {@code
 * toString} deliberately omits the secret.
 */
public record R2ParentToken(@NonNull String accessKeyId, @NonNull String secretAccessKey) {

  /** First 8 hex characters of SHA-256 over the secret; safe to place in cache keys and logs. */
  public static String fingerprint(String secret) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 4);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  @Override
  public String toString() {
    return "R2ParentToken{accessKeyId=" + accessKeyId + ", secret=<redacted>}";
  }
}
