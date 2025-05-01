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

public enum AuthenticationType {

  /** Polaris will be the only identity provider. */
  INTERNAL,

  /**
   * Polaris will delegate authentication to an external identity provider (e.g., Keycloak). The
   * internal token endpoint will be deactivated.
   */
  EXTERNAL,

  /**
   * Polaris will act as an identity provider, but it will also support external authentication. The
   * internal token endpoint will be activated and will always be tried first. If the token issuer
   * is not Polaris, the token will be passed to the external identity provider for validation.
   */
  MIXED,
}
