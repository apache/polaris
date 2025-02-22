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
package org.apache.polaris.service.persistence.fdb.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.dao.NamespaceDao;

@ApplicationScoped
public class FdbNamespaceDao implements NamespaceDao {
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisMetaStoreSession metaStoreSession;

  @Inject
  public FdbNamespaceDao(
      PolarisMetaStoreManager metaStoreManager, PolarisMetaStoreSession metaStoreSession) {
    this.metaStoreManager = metaStoreManager;
    this.metaStoreSession = metaStoreSession;
  }

  @Override
  public void save(NamespaceEntity namespace, List<PolarisEntityCore> catalogPath) {
    var returnedEntity =
        PolarisEntity.of(
            metaStoreManager.createEntityIfNotExists(metaStoreSession, catalogPath, namespace));

    if (returnedEntity == null) {
      throw new AlreadyExistsException(
          "Cannot create namespace %s. Namespace already exists", namespace);
    }
  }

  @Override
  public void update(NamespaceEntity namespace, List<PolarisEntityCore> catalogPath) {
    var returnedEntity =
        Optional.ofNullable(
                metaStoreManager
                    .updateEntityPropertiesIfNotChanged(metaStoreSession, catalogPath, namespace)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new RuntimeException("Concurrent modification of namespace: " + namespace);
    }
  }

  @Override
  public NamespaceEntity get(String namespaceId) {
    // todo
    return null;
  }

  @Override
  public NamespaceEntity get(Namespace namespace) {
    // todo
    return null;
  }

  @Override
  public boolean delete(
      List<PolarisEntityCore> catalogPath, PolarisEntity leafEntity, boolean cleanup) {
    var dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            metaStoreSession, catalogPath, leafEntity, Map.of(), cleanup);
    if (!dropEntityResult.isSuccess() && dropEntityResult.failedBecauseNotEmpty()) {
      throw new NamespaceNotEmptyException("Namespace is not empty");
    }

    return dropEntityResult.isSuccess();
  }
}
