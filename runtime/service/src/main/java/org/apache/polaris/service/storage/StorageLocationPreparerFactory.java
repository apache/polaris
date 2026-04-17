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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

@ApplicationScoped
public class StorageLocationPreparerFactory {

  private static final StorageLocationPreparer NO_OP = storageLocations -> {};
  private static final StorageLocationPreparerFactory NO_OP_FACTORY =
      new StorageLocationPreparerFactory();

  private final Instance<StorageLocationPreparer> preparers;

  @Inject
  public StorageLocationPreparerFactory(@Any Instance<StorageLocationPreparer> preparers) {
    this.preparers = preparers;
  }

  private StorageLocationPreparerFactory() {
    this.preparers = null;
  }

  public StorageLocationPreparer create(@Nonnull PolarisStorageConfigurationInfo storageConfig) {
    if (preparers == null) {
      return NO_OP;
    }
    String key = storageConfig.getStorageType().name();
    Instance<StorageLocationPreparer> selected = preparers.select(Identifier.Literal.of(key));
    return selected.isResolvable() ? selected.get() : NO_OP;
  }

  public static StorageLocationPreparerFactory noOp() {
    return NO_OP_FACTORY;
  }
}
