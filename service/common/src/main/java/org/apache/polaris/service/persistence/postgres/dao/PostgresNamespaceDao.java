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
package org.apache.polaris.service.persistence.postgres.dao;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.NamespaceDao;

public class PostgresNamespaceDao implements NamespaceDao {
  @Override
  public void save(NamespaceEntity namespace, List<PolarisEntityCore> catalogPath) {
    // jdbc implementation, we could use JPA like EclipseLink as well.
    // execute("INSERT INTO %s (catalog_name, namespace, property, value) VALUES (?, ?, ?, ?)");
  }

  @Override
  public void update(NamespaceEntity namespace, List<PolarisEntityCore> catalogPath) {
    // execute("INSERT INTO %s (catalog_name, namespace, property, value) VALUES (?, ?, ?, ?)");
  }

  @Override
  public NamespaceEntity get(String namespaceId) {
    return null;
  }

  @Override
  public NamespaceEntity get(Namespace namespace) {
    return null;
  }

  @Override
  public boolean delete(
      List<PolarisEntityCore> catalogPath, PolarisEntity leafEntity, boolean cleanup) {
    return false;
  }
}
