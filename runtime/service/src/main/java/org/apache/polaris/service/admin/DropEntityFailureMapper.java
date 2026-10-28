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
package org.apache.polaris.service.admin;

import java.util.function.Supplier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.jspecify.annotations.Nullable;

/** Maps {@link DropEntityResult} failures from admin delete operations to REST exceptions. */
final class DropEntityFailureMapper {

  private DropEntityFailureMapper() {}

  static void throwIfFailed(
      DropEntityResult result, Supplier<String> entityLabel, @Nullable String undroppableMessage) {
    if (result.isSuccess()) {
      return;
    }

    String label = entityLabel.get();
    switch (result.getReturnStatus()) {
      case ENTITY_UNDROPPABLE ->
          throw new BadRequestException(
              "%s", undroppableMessage != null ? undroppableMessage : label + " cannot be dropped");
      case TARGET_ENTITY_CONCURRENTLY_MODIFIED ->
          throw new BadRequestException(
              "%s cannot be dropped, concurrent modification detected. Please try again", label);
      case ENTITY_NOT_FOUND -> throw new NotFoundException("%s not found", label);
      case CATALOG_NOT_EMPTY, NAMESPACE_NOT_EMPTY ->
          throw new BadRequestException("%s cannot be dropped, it is not empty", label);
      default ->
          throw new BadRequestException(
              "%s cannot be dropped: %s", label, result.getReturnStatus());
    }
  }
}
