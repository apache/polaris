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

import jakarta.annotation.Nonnull;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRecoveryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskRecoveryManager.class);

  public static void recoverPendingTasks(
      MetaStoreManagerFactory metaStoreManagerFactory,
      String executorId,
      TaskExecutor taskExecutor) {
    recoverPendingTasks(
        metaStoreManagerFactory,
        executorId,
        taskExecutor,
        new PolarisConfigurationStore() {},
        Clock.system(ZoneId.systemDefault()));
  }

  public static void recoverPendingTasks(
      @Nonnull MetaStoreManagerFactory metaStoreManagerFactory,
      @Nonnull String executorId,
      @Nonnull TaskExecutor taskExecutor,
      @Nonnull PolarisConfigurationStore configurationStore,
      @Nonnull Clock clock) {
    for (Map.Entry<String, PolarisMetaStoreManager> entry :
        metaStoreManagerFactory.getMetaStoreManagerMap().entrySet()) {
      RealmContext realmContext = entry::getKey;
      PolarisMetaStoreManager metaStoreManager = entry.getValue();
      BasePersistence metastore =
          metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
      // Construct a PolarisCallContext since the original one has lost
      PolarisCallContext polarisCallContext =
          new PolarisCallContext(
              metastore, new PolarisDefaultDiagServiceImpl(), configurationStore, clock);
      EntitiesResult entitiesResult =
          metaStoreManager.loadTasks(polarisCallContext, executorId, 20, true);
      if (entitiesResult.getReturnStatus() == BaseResult.ReturnStatus.SUCCESS) {
        entitiesResult.getEntities().stream()
            .map(TaskEntity::of)
            .forEach(
                entity -> {
                  // Construct a CallContext since the original one has lost
                  CallContext callContext = CallContext.of(realmContext, polarisCallContext);
                  taskExecutor.addTaskHandlerContext(entity.getId(), callContext);
                });
      } else {
        LOGGER.error("Failed to recover pending tasks for {}", executorId);
      }
    }
  }
}
