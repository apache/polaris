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
package org.apache.polaris.core.persistence.bootstrap;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * A utility to parse and provide credentials for Polaris realms and principals during a bootstrap
 * phase.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableRootCredentialsSet.class)
@JsonDeserialize(as = ImmutableRootCredentialsSet.class)
@Value.Style(jdkOnly = true)
public interface RootCredentialsSet {

  RootCredentialsSet EMPTY = ImmutableRootCredentialsSet.builder().build();

  String SYSTEM_PROPERTY = "polaris.bootstrap.credentials";
  String ENVIRONMENT_VARIABLE = "POLARIS_BOOTSTRAP_CREDENTIALS";

  /**
   * Parse credentials from the system property {@value #SYSTEM_PROPERTY} or the environment
   * variable {@value #ENVIRONMENT_VARIABLE}, whichever is set.
   *
   * <p>See {@link #fromString(String)} for the expected format.
   */
  static RootCredentialsSet fromEnvironment() {
    return fromString(
        System.getProperty(SYSTEM_PROPERTY, System.getenv().get(ENVIRONMENT_VARIABLE)));
  }

  /**
   * Parse a string of credentials in the format:
   *
   * <pre>
   * realm1,client1,secret1;realm2,client2,secret2;...
   * </pre>
   */
  static RootCredentialsSet fromString(@Nullable String credentialsString) {
    return credentialsString != null && !credentialsString.isBlank()
        ? fromList(Splitter.on(';').trimResults().splitToList(credentialsString))
        : EMPTY;
  }

  /**
   * Parse a list of credentials; each element should be in the format: {@code
   * realm,clientId,clientSecret}.
   */
  static RootCredentialsSet fromList(List<String> credentialsList) {
    Map<String, RootCredentials> credentials = new HashMap<>();
    for (String triplet : credentialsList) {
      if (!triplet.isBlank()) {
        List<String> parts = Splitter.on(',').trimResults().splitToList(triplet);
        if (parts.size() != 3) {
          throw new IllegalArgumentException("Invalid credentials format: " + triplet);
        }
        String realm = parts.get(0);
        RootCredentials creds = ImmutableRootCredentials.of(parts.get(1), parts.get(2));
        if (credentials.containsKey(realm)) {
          throw new IllegalArgumentException("Duplicate realm: " + realm);
        }
        credentials.put(realm, creds);
      }
    }
    return credentials.isEmpty() ? EMPTY : ImmutableRootCredentialsSet.of(credentials);
  }

  /**
   * Parse credentials set from any file or JRE resource containing a valid YAML or JSON credentials
   * file.
   *
   * <p>Note: HTTP and other remote URLs are not allowed.
   *
   * <p>The expected YAML format is:
   *
   * <pre>
   * realm1:
   *   client-id: client1
   *   client-secret: secret1
   * realm2:
   *   client-id: client2
   *   client-secret: secret2
   * # etc.
   * </pre>
   *
   * Multiple YAMl documents are also supported; all documents will be merged into a single set of
   * credentials.
   *
   * <p>The expected JSON format is:
   *
   * <pre>
   * {
   *   "realm1": {
   *     "client-id": "client1",
   *     "client-secret": "secret1"
   *   },
   *   "realm2": {
   *     "client-id": "client2",
   *     "client-secret": "secret2"
   *   }
   * }
   * </pre>
   */
  static RootCredentialsSet fromUri(URI uri) {
    Preconditions.checkNotNull(uri);
    Preconditions.checkArgument(
        Strings.isNullOrEmpty(uri.getHost()),
        "Remote URIs are not allowed for RootCredentialsSet: %s",
        uri);
    try (InputStream is = uri.toURL().openStream()) {
      return fromInputStream(is);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to read credentials from " + uri, e);
    }
  }

  private static RootCredentialsSet fromInputStream(InputStream in) throws IOException {
    YAMLFactory factory = new YAMLFactory();
    ObjectMapper mapper = new ObjectMapper(factory).configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    try (var parser = factory.createParser(in)) {
      var values = mapper.readValues(parser, RootCredentialsSet.class);
      var builder = ImmutableRootCredentialsSet.builder();
      while (values.hasNext()) {
        builder.putAllCredentials(values.next().credentials());
      }
      return builder.build();
    }
  }

  /** Get all the credentials contained in this set, keyed by realm identifier. */
  @JsonAnyGetter
  @JsonAnySetter
  @Value.Parameter(order = 0)
  Map<String, RootCredentials> credentials();
}
