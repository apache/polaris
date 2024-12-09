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
package org.apache.polaris.core.persistence;

import java.util.Optional;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

public class EntityNotFoundException extends RuntimeException {
  private final PolarisEntityType entityType;
  private final Optional<PolarisEntitySubType> subType;
  private final String name;

  // TODO This exception currently serves as a generic one which is then transformed into concrete,
  //  but Iceberg specific exception. That exception mapping should really be handled at the REST
  //  service level using "proper" exception mappers, based on Polaris specific exceptions.
  // TODO Unify/cleanup this exception w/ `ResolverException`

  public EntityNotFoundException(
      PolarisEntityType entityType, Optional<PolarisEntitySubType> subType, String name) {
    super(
        subType.map(PolarisEntitySubType::readableName).orElseGet(entityType::readableName)
            + " "
            + name);
    this.entityType = entityType;
    this.subType = subType;
    this.name = name;
  }

  public EntityNotFoundException(PolarisEntityType entityType, String name) {
    this(entityType, Optional.empty(), name);
  }

  public RuntimeException asGenericIcebergNotFoundException() {
    throw new NotFoundException(
        "%s does not exist: %s",
        subType.map(PolarisEntitySubType::readableName).orElseGet(entityType::readableName), name);
  }

  public RuntimeException asSpecializedIcebergNotFoundException() {
    // Note: the exception messages are mandatory for the Iceberg catalog-tests.
    switch (entityType) {
      case NAMESPACE:
        return new NoSuchNamespaceException("Namespace does not exist: %s", name);
      case TABLE_LIKE:
        return subType
            .map(
                sub -> {
                  switch (sub) {
                    case TABLE:
                      return new NoSuchTableException("Table does not exist: %s", name);
                    case VIEW:
                      return new NoSuchViewException("View does not exist: %s", name);
                    default:
                      return asGenericIcebergNotFoundException();
                  }
                })
            .orElseGet(this::asGenericIcebergNotFoundException);
      default:
        return asGenericIcebergNotFoundException();
    }
  }
}
