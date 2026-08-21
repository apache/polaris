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
package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTMessage;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.collection.AttributeMap.AttributeKey;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.LoadTagResponse;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Verifies that every {@link AttributeKey} constant declared in {@link EventAttributes} has a value
 * type from the approved allowlist.
 */
class EventAttributesTest {

  private static final Set<Class<?>> ALLOWED_TYPES =
      Set.of(
          // Primitives
          String.class,
          Boolean.class,
          Number.class,
          // Iceberg types
          RESTMessage.class,
          Namespace.class,
          TableIdentifier.class,
          TableMetadata.class,
          ViewMetadata.class,
          // Polaris admin model types
          Catalog.class,
          Principal.class,
          PrincipalRole.class,
          CatalogRole.class,
          GrantResource.class,
          UpdatePrincipalRequest.class,
          CreatePrincipalRoleRequest.class,
          UpdatePrincipalRoleRequest.class,
          UpdateCatalogRequest.class,
          UpdateCatalogRoleRequest.class,
          AddGrantRequest.class,
          RevokeGrantRequest.class,
          PolarisPrivilege.class,
          // Polaris service types
          CommitViewRequest.class,
          GenericTable.class,
          CreateGenericTableRequest.class,
          CreatePolicyRequest.class,
          CreateTagRequest.class,
          UpdateTagRequest.class,
          LoadTagResponse.class,
          ListTagsResponse.class,
          UpdatePolicyRequest.class,
          LoadPolicyResponse.class,
          AttachPolicyRequest.class,
          DetachPolicyRequest.class,
          GetApplicablePoliciesResponse.class,
          NotificationRequest.class);

  private static final Set<Class<?>> COLLECTION_TYPES = Set.of(List.class, Set.class, Map.class);

  private static final ClassValue<Boolean> ALLOWED_CACHE =
      new ClassValue<>() {
        @Override
        protected Boolean computeValue(@NonNull Class<?> type) {
          return ALLOWED_TYPES.stream().anyMatch(allowed -> allowed.isAssignableFrom(type));
        }
      };

  @Test
  void allEventAttributeValueTypesAreAllowed() {
    for (Field field : EventAttributes.class.getDeclaredFields()) {
      if (!Modifier.isPublic(field.getModifiers())
          || !Modifier.isStatic(field.getModifiers())
          || !AttributeKey.class.isAssignableFrom(field.getType())) {
        continue;
      }

      Type genericFieldType = field.getGenericType();
      assertThat(genericFieldType).isInstanceOf(ParameterizedType.class);

      Type valueType = ((ParameterizedType) genericFieldType).getActualTypeArguments()[0];
      assertThat(isAllowed(valueType)).isTrue();
    }
  }

  private static boolean isAllowed(Type type) {
    if (type instanceof ParameterizedType pt) {
      Class<?> rawType = (Class<?>) pt.getRawType();
      if (COLLECTION_TYPES.contains(rawType)) {
        for (Type typeArg : pt.getActualTypeArguments()) {
          if (!isAllowed(typeArg)) {
            return false;
          }
        }
        return true;
      }
      return ALLOWED_CACHE.get(rawType);
    } else if (type instanceof Class<?> clazz) {
      return ALLOWED_CACHE.get(clazz);
    }
    return false;
  }
}
