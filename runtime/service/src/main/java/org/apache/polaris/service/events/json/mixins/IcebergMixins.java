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

package org.apache.polaris.service.events.json.mixins;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.catalog.Namespace;

/** Mixins for Iceberg classes we don't control, to keep JSON concise. */
public final class IcebergMixins {

  // Private constructor to prevent instantiation
  private IcebergMixins() {}

  /** Serializes Namespace as an object like: "namespace": ["sales", "north.america"] */
  public abstract static class NamespaceMixin {
    @JsonProperty("namespace")
    public abstract String[] levels();
  }

  /**
   * Serializes TableIdentifier as a scalar string like: {"namespace": ["sales", "north.america"],
   * "name": "transactions"}
   */
  public abstract static class TableIdentifierMixin {
    @JsonProperty("namespace")
    public abstract Namespace namespace();

    @JsonProperty("name")
    public abstract String name();
  }
}
