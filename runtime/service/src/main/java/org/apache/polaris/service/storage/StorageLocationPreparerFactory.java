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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.apache.polaris.service.storage.gcs.GcsStorageLocationPreparer;

@ApplicationScoped
public class StorageLocationPreparerFactory {

  private static final StorageLocationPreparer NO_OP = (tableLocation, tableProperties) -> {};
  private static final StorageLocationPreparerFactory NO_OP_FACTORY =
      new StorageLocationPreparerFactory();

  private final StorageConfiguration storageConfiguration;
  private final Clock clock;

  @Inject
  public StorageLocationPreparerFactory(StorageConfiguration storageConfiguration, Clock clock) {
    this.storageConfiguration = storageConfiguration;
    this.clock = clock;
  }

  private StorageLocationPreparerFactory() {
    this.storageConfiguration = null;
    this.clock = Clock.systemUTC();
  }

  public StorageLocationPreparer create(@Nonnull PolarisStorageConfigurationInfo storageConfig) {
    if (storageConfig instanceof GcpStorageConfigurationInfo && storageConfiguration != null) {
      return new GcsStorageLocationPreparer(storageConfiguration.gcpCredentialsSupplier(clock));
    }
    return NO_OP;
  }

  public static StorageLocationPreparerFactory noOp() {
    return NO_OP_FACTORY;
  }
}
