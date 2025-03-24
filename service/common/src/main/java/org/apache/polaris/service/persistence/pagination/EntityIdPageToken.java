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
package org.apache.polaris.service.persistence.pagination;

import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.jpa.models.ModelEntity;

/**
 * A {@link PageToken} implementation that tracks the greatest ID from either {@link
 * PolarisBaseEntity} or {@link ModelEntity} objects supplied in updates. Entities are meant to be
 * filtered during listing such that only entities with and ID greater than the ID of the token are
 * returned.
 */
public class EntityIdPageToken extends PageToken {
  public long id;

  private EntityIdPageToken(long id, int pageSize) {
    this.id = id;
    this.pageSize = pageSize;
    validate();
  }

  /** The minimum ID that could be attached to an entity */
  private static final long MINIMUM_ID = 0;

  /** The entity ID to use to start with. */
  private static final long BASE_ID = MINIMUM_ID - 1;

  @Override
  protected List<String> getComponents() {
    return List.of(String.valueOf(id), String.valueOf(pageSize));
  }

  /** Get a new `EntityIdPageTokenBuilder` instance */
  public static PageTokenBuilder<EntityIdPageToken> builder() {
    return new EntityIdPageToken.EntityIdPageTokenBuilder();
  }

  @Override
  protected PageTokenBuilder<?> getBuilder() {
    return EntityIdPageToken.builder();
  }

  /** A {@link PageTokenBuilder} implementation for {@link EntityIdPageToken} */
  public static class EntityIdPageTokenBuilder extends PageTokenBuilder<EntityIdPageToken> {

    @Override
    public String tokenPrefix() {
      return "polaris-entity-id";
    }

    @Override
    public int expectedComponents() {
      // id, pageSize
      return 2;
    }

    @Override
    protected EntityIdPageToken fromStringComponents(List<String> components) {
      return new EntityIdPageToken(
          Integer.parseInt(components.get(0)), Integer.parseInt(components.get(1)));
    }

    @Override
    protected EntityIdPageToken fromLimitImpl(int limit) {
      return new EntityIdPageToken(BASE_ID, limit);
    }
  }

  @Override
  public PageToken updated(List<?> newData) {
    if (newData == null || newData.size() < this.pageSize) {
      return DONE;
    } else {
      var head = newData.get(0);
      if (head instanceof ModelEntity) {
        return new EntityIdPageToken(
            ((ModelEntity) newData.get(newData.size() - 1)).getId(), this.pageSize);
      } else if (head instanceof PolarisBaseEntity) {
        return new EntityIdPageToken(
            ((PolarisBaseEntity) newData.get(newData.size() - 1)).getId(), this.pageSize);
      } else {
        throw new IllegalArgumentException(
            "Cannot build a page token from: " + newData.get(0).getClass().getSimpleName());
      }
    }
  }

  @Override
  public PageToken withPageSize(Integer pageSize) {
    if (pageSize == null) {
      return new EntityIdPageToken(BASE_ID, this.pageSize);
    } else {
      return new EntityIdPageToken(this.id, pageSize);
    }
  }
}
