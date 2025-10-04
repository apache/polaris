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

import jakarta.enterprise.util.Nonbinding;
import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Container annotation for multiple {@link SupportsAuthType} annotations.
 *
 * <p>This annotation allows a single {@link ConnectionCredentialProvider} implementation to support
 * multiple authentication types.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @SupportsAuthType(AuthenticationType.IMPLICIT)
 * @SupportsAuthType(AuthenticationType.NONE)
 * public class NoOpConnectionCredentialProvider implements ConnectionCredentialProvider {
 *   // Handles both IMPLICIT and NONE authentication (no credential transformation)
 * }
 * }</pre>
 */
@Qualifier
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SupportsAuthTypes {
  @Nonbinding
  SupportsAuthType[] value();
}
