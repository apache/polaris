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
package org.apache.polaris.extensions.lineage;

import java.util.Objects;
import java.util.OptionalLong;

/** A dataset participating in lineage. */
public record LineageDataset(
    String catalog, String namespace, String name, OptionalLong polarisEntityId) {
  public LineageDataset {
    Objects.requireNonNull(catalog, "catalog must be non-null");
    Objects.requireNonNull(namespace, "namespace must be non-null");
    Objects.requireNonNull(name, "name must be non-null");
    Objects.requireNonNull(polarisEntityId, "polarisEntityId must be non-null");
  }

  public LineageDataset(String catalog, String namespace, String name) {
    this(catalog, namespace, name, OptionalLong.empty());
  }
}
