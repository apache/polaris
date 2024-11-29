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
package org.apache.polaris.jpa.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.apache.polaris.core.entity.PolarisBaseEntity;

/**
 * EntityChangeTracking model representing some attributes of a Polaris Entity. This is used to
 * exchange entity information with ENTITIES_CHANGE_TRACKING table
 */
@Entity
@Table(name = "ENTITIES_CHANGE_TRACKING")
public class ModelEntityChangeTracking {
  // the id of the catalog associated to that entity. NULL_ID if this entity is top-level like
  // a catalog
  @Id private long catalogId;

  // the id of the entity which was resolved
  @Id private long id;

  // the version that this entity had when it was resolved
  private int entityVersion;

  // current version for that entity, will be monotonically incremented
  private int grantRecordsVersion;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

  public ModelEntityChangeTracking() {}

  public ModelEntityChangeTracking(PolarisBaseEntity entity) {
    this.catalogId = entity.getCatalogId();
    this.id = entity.getId();
    this.entityVersion = entity.getEntityVersion();
    this.grantRecordsVersion = entity.getGrantRecordsVersion();
  }

  public long getCatalogId() {
    return catalogId;
  }

  public long getId() {
    return id;
  }

  public int getEntityVersion() {
    return entityVersion;
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public void update(PolarisBaseEntity entity) {
    this.entityVersion = entity.getEntityVersion();
    this.grantRecordsVersion = entity.getGrantRecordsVersion();
  }
}
