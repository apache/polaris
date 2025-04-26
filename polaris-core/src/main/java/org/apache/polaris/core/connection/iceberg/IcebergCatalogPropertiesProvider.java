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
package org.apache.polaris.core.connection.iceberg;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * Configuration wrappers which ultimately translate their contents into Iceberg properties and
 * which may hold other nested configuration wrapper objects implement this interface to allow
 * delegating type-specific configuration translation logic to subclasses instead of needing to
 * expose the internals of deeply nested configuration objects to a visitor class.
 */
public interface IcebergCatalogPropertiesProvider {
  @Nonnull
  Map<String, String> asIcebergCatalogProperties(UserSecretsManager secretsManager);
}
