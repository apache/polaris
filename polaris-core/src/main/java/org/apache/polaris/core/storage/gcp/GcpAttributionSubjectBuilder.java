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
package org.apache.polaris.core.storage.gcp;

/**
 * Builds the {@code sub} claim for GCS principal-attribution JWTs as {@code <realm>/<principal>},
 * within GCP's 127-character {@code google.subject} limit.
 *
 * <p>The character budget mirrors the AWS session-name builder: one character is reserved for the
 * separator, then each field receives an equal share of the remainder, and budget unused by a short
 * field flows to the other. ISO control characters and the {@code /} separator are stripped from
 * each field so the subject stays unambiguously parseable, and the {@code unknown} placeholder
 * substitutes null/empty fields so the subject shape stays stable.
 */
public final class GcpAttributionSubjectBuilder {

  /** GCP limit for the {@code google.subject} attribute of a federated identity. */
  public static final int MAX_SUBJECT_LENGTH = 127;

  static final String SEPARATOR = "/";

  static final String VALUE_UNKNOWN = "unknown";

  private GcpAttributionSubjectBuilder() {}

  /**
   * Builds the attribution subject {@code <realm>/<principal>}, guaranteed to be at most {@value
   * #MAX_SUBJECT_LENGTH} characters.
   *
   * @param realm the realm identifier (gets first-half budget priority)
   * @param principalName the Polaris principal name
   * @return the subject string
   */
  public static String buildSubject(String realm, String principalName) {
    String cleanRealm = sanitize(realm);
    String cleanPrincipal = sanitize(principalName);

    int budget = MAX_SUBJECT_LENGTH - SEPARATOR.length();
    int remaining = budget;

    int realmAlloc = remaining / 2;
    int realmUsed = Math.min(cleanRealm.length(), realmAlloc);
    remaining -= realmUsed;

    int principalUsed = Math.min(cleanPrincipal.length(), remaining);
    remaining -= principalUsed;

    // Carry-forward: if the principal left budget unused, the realm may take more than its
    // initial half-share.
    int realmFinal = Math.min(cleanRealm.length(), realmUsed + remaining);

    return cleanRealm.substring(0, realmFinal)
        + SEPARATOR
        + cleanPrincipal.substring(0, principalUsed);
  }

  private static String sanitize(String value) {
    if (value == null || value.isEmpty()) {
      return VALUE_UNKNOWN;
    }
    StringBuilder cleaned = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      // Drop control chars and the separator itself so the subject stays unambiguously
      // <realm>/<principal>: a value containing '/' would otherwise let an audit-log consumer
      // mis-split it (e.g. principal "a/b" read as realm "a", principal "b").
      if (!Character.isISOControl(c) && c != '/') {
        cleaned.append(c);
      }
    }
    return cleaned.length() == 0 ? VALUE_UNKNOWN : cleaned.toString();
  }
}
