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
package org.apache.polaris.service.credentials.connection;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;

/**
 * CDI qualifier to indicate which authentication type a {@link ConnectionCredentialVendor}
 * supports.
 *
 * <p>This annotation allows the credential manager to automatically select the appropriate vendor
 * based on the authentication type specified in the connection configuration.
 *
 * <p>This annotation is repeatable, allowing a single vendor to support multiple authentication
 * types. When multiple {@link AuthType} annotations are applied, the vendor will be selected for
 * any of the specified authentication types.
 *
 * <p>Example usage for single auth type:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @AuthType(AuthenticationType.SIGV4)
 * @Priority(100)
 * public class SigV4ConnectionCredentialVendor implements ConnectionCredentialVendor {
 *   // AWS STS AssumeRole logic for SigV4 authentication
 * }
 * }</pre>
 *
 * <p>Example usage for multiple auth types:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @AuthType(AuthenticationType.IMPLICIT)
 * @AuthType(AuthenticationType.BEARER)
 * @Priority(100)
 * public class PassThroughCredentialVendor implements ConnectionCredentialVendor {
 *   // No transformation needed for these auth types
 * }
 * }</pre>
 */
@Qualifier
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(AuthTypes.class)
public @interface AuthType {

  /** The authentication type this vendor supports. */
  AuthenticationType value();

  /** Helper for creating {@link AuthType} qualifiers programmatically. */
  final class Literal extends AnnotationLiteral<AuthType> implements AuthType {
    private final AuthenticationType value;

    public static Literal of(AuthenticationType value) {
      return new Literal(value);
    }

    private Literal(AuthenticationType value) {
      this.value = value;
    }

    @Override
    public AuthenticationType value() {
      return value;
    }
  }
}
