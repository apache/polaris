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

package org.apache.polaris.service.catalog.tag;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogTagApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;

@Decorator
@Priority(1000)
public class CatalogTagEventServiceDelegator
    implements PolarisCatalogTagApiService, CatalogAdapter {

  @Inject @Delegate TagCatalogAdapter delegate;
  @Inject PolarisEventDispatcher polarisEventDispatcher;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createTag(
      String prefix,
      CreateTagRequest createTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_CREATE_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_CREATE_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.CREATE_TAG_REQUEST, createTagRequest)
                  .build()));
    }
    Response resp =
        delegate.createTag(prefix, createTagRequest, idempotencyKey, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_CREATE_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_CREATE_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG, (Tag) resp.getEntity())
                  .build()));
    }
    return resp;
  }

  @Override
  public Response listTags(
      String prefix,
      Boolean pagination,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_LIST_TAGS)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_LIST_TAGS,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .build()));
    }
    Response resp =
        delegate.listTags(prefix, pagination, pageToken, pageSize, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_LIST_TAGS)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_LIST_TAGS,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.LIST_TAGS_RESPONSE, (ListTagsResponse) resp.getEntity())
                  .build()));
    }
    return resp;
  }

  @Override
  public Response loadTag(
      String prefix, String tagName, RealmContext realmContext, SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_LOAD_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_LOAD_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .build()));
    }
    Response resp = delegate.loadTag(prefix, tagName, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_LOAD_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_LOAD_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG, (Tag) resp.getEntity())
                  .build()));
    }
    return resp;
  }

  @Override
  public Response updateTag(
      String prefix,
      String tagName,
      UpdateTagRequest updateTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_UPDATE_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_UPDATE_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.UPDATE_TAG_REQUEST, updateTagRequest)
                  .build()));
    }
    Response resp =
        delegate.updateTag(
            prefix, tagName, updateTagRequest, idempotencyKey, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_UPDATE_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_UPDATE_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG, (Tag) resp.getEntity())
                  .build()));
    }
    return resp;
  }

  @Override
  public Response renameTag(
      String prefix,
      RenameTagRequest renameTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_RENAME_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_RENAME_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.RENAME_TAG_REQUEST, renameTagRequest)
                  .build()));
    }
    Response resp =
        delegate.renameTag(prefix, renameTagRequest, idempotencyKey, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_RENAME_TAG)) {
      // A rename answers 204, so there is no definition in the response to report; the request
      // carries both names, which is what a listener needs.
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_RENAME_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.RENAME_TAG_REQUEST, renameTagRequest)
                  .build()));
    }
    return resp;
  }

  @Override
  public Response dropTag(
      String prefix,
      String tagName,
      Boolean detachAll,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_DROP_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_DROP_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.DETACH_ALL, detachAll)
                  .build()));
    }
    Response resp = delegate.dropTag(prefix, tagName, detachAll, realmContext, securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_DROP_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_DROP_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, resolvedCatalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.DETACH_ALL, detachAll)
                  .build()));
    }
    return resp;
  }

  @Override
  public Response assignTag(
      String prefix,
      String tagName,
      TargetType targetType,
      AssignTagRequest assignTagRequest,
      String namespace,
      String targetName,
      String column,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(targetType, namespace, targetName, column);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_ASSIGN_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_ASSIGN_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG_ASSIGNMENT_TARGET, target)
                  .put(EventAttributes.ASSIGN_TAG_REQUEST, assignTagRequest)
                  .build()));
    }
    Response resp =
        delegate.assignTag(
            prefix,
            tagName,
            targetType,
            assignTagRequest,
            namespace,
            targetName,
            column,
            realmContext,
            securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_ASSIGN_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_ASSIGN_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG_ASSIGNMENT_TARGET, target)
                  .put(EventAttributes.ASSIGN_TAG_REQUEST, assignTagRequest)
                  .build()));
    }
    return resp;
  }

  @Override
  public Response unassignTag(
      String prefix,
      String tagName,
      TargetType targetType,
      String namespace,
      String targetName,
      String column,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(targetType, namespace, targetName, column);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_UNASSIGN_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.BEFORE_UNASSIGN_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG_ASSIGNMENT_TARGET, target)
                  .build()));
    }
    Response resp =
        delegate.unassignTag(
            prefix,
            tagName,
            targetType,
            namespace,
            targetName,
            column,
            realmContext,
            securityContext);
    if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_UNASSIGN_TAG)) {
      polarisEventDispatcher.dispatch(
          new PolarisEvent(
              PolarisEventType.AFTER_UNASSIGN_TAG,
              eventMetadataFactory.create(),
              ImmutableAttributeMap.builder()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.TAG_NAME, tagName)
                  .put(EventAttributes.TAG_ASSIGNMENT_TARGET, target)
                  .build()));
    }
    return resp;
  }

  // getObjectTags and listObjectsByTag are not implemented yet; the default
  // PolarisCatalogTagApiService methods return 501 and are not decorated here.
}
