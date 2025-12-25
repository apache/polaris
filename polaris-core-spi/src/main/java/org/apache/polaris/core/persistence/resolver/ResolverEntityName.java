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
package org.apache.polaris.core.persistence.resolver;

import java.util.Objects;
import org.apache.polaris.core.entity.PolarisEntityType;

/** Simple class to represent the name of an entity to resolve */
public class ResolverEntityName {

  // type of the entity
  private final PolarisEntityType entityType;

  // the name of the entity
  private final String entityName;

  // true if we should not fail while resolving this entity
  private final boolean isOptional;

  public ResolverEntityName(PolarisEntityType entityType, String entityName, boolean isOptional) {
    this.entityType = entityType;
    this.entityName = entityName;
    this.isOptional = isOptional;
  }

  public PolarisEntityType getEntityType() {
    return entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  public boolean isOptional() {
    return isOptional;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ResolverEntityName)) return false;
    ResolverEntityName that = (ResolverEntityName) o;
    return getEntityType() == that.getEntityType()
        && Objects.equals(getEntityName(), that.getEntityName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getEntityType(), getEntityName());
  }
}
