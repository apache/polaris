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
package org.apache.polaris.service.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.io.FileIOFactory;

public class TaskFileIOSupplier implements Function<TaskEntity, FileIO> {
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final FileIOFactory fileIOFactory;
  private final PolarisConfigurationStore configurationStore;

  public TaskFileIOSupplier(
      MetaStoreManagerFactory metaStoreManagerFactory,
      FileIOFactory fileIOFactory,
      PolarisConfigurationStore configurationStore) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOFactory = fileIOFactory;
    this.configurationStore = configurationStore;
  }

  @Override
  public FileIO apply(TaskEntity task) {
    Map<String, String> internalProperties = task.getInternalPropertiesAsMap();
    String location = internalProperties.get(PolarisTaskConstants.STORAGE_LOCATION);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(
            CallContext.getCurrentContext().getRealmContext());
    Map<String, String> properties = new HashMap<>(internalProperties);

    Boolean skipCredentialSubscopingIndirection =
        configurationStore.getConfiguration(
            null,
            PolarisConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION.key,
            PolarisConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION.defaultValue);

    if (!skipCredentialSubscopingIndirection) {
      properties.putAll(
          metaStoreManagerFactory
              .getOrCreateStorageCredentialCache(CallContext.getCurrentContext().getRealmContext())
              .getOrGenerateSubScopeCreds(
                  metaStoreManager,
                  CallContext.getCurrentContext().getPolarisCallContext(),
                  task,
                  true,
                  Set.of(location),
                  Set.of(location)));
    }
    String ioImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    return fileIOFactory.loadFileIO(ioImpl, properties);
  }
}
