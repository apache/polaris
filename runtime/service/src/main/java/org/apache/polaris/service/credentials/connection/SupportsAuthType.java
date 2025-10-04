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

/**
 * CDI qualifier to indicate which authentication type(s) a {@link ConnectionCredentialProvider}
 * supports.
 *
 * <p>This annotation allows the credential manager to automatically select the appropriate provider
 * based on the authentication type specified in the connection configuration.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @SupportsAuthType(AuthenticationType.SIGV4)
 * public class SigV4ConnectionCredentialProvider implements ConnectionCredentialProvider {
 *   // AWS STS AssumeRole logic for SigV4 authentication
 * }
 * }</pre>
 */
@Qualifier
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SupportsAuthTypes.class)
public @interface SupportsAuthType {

  /** The authentication type this provider supports. */
  AuthenticationType value();

  /** Helper for creating {@link SupportsAuthType} qualifiers programmatically. */
  final class Literal extends AnnotationLiteral<SupportsAuthType> implements SupportsAuthType {
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
