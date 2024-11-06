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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.jackson.Discoverable;
import java.security.Principal;

/**
 * Extension of the {@link Authenticator} interface that extends {@link Discoverable} so
 * implementations can be discovered using the mechanisms described in <a
 * href="https://www.dropwizard.io/en/stable/manual/configuration.html#polymorphic-configuration">the
 * manual</a>. The default implementation is {@link TestInlineBearerTokenPolarisAuthenticator}.
 *
 * @param <C>
 * @param <P>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public interface DiscoverableAuthenticator<C, P extends Principal> extends Authenticator<C, P> {}
