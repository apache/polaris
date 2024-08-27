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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;

public final class Serializers {
  private Serializers() {}

  public static void registerSerializers(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(CreateCatalogRequest.class, new CreateCatalogRequestDeserializer());
    module.addDeserializer(CreatePrincipalRequest.class, new CreatePrincipalRequestDeserializer());
    module.addDeserializer(
        CreatePrincipalRoleRequest.class, new CreatePrincipalRoleRequestDeserializer());
    module.addDeserializer(
        GrantPrincipalRoleRequest.class, new GrantPrincipalRoleRequestDeserializer());
    module.addDeserializer(
        CreateCatalogRoleRequest.class, new CreateCatalogRoleRequestDeserializer());
    module.addDeserializer(
        GrantCatalogRoleRequest.class, new GrantCatalogRoleRequestDeserializer());
    module.addDeserializer(AddGrantRequest.class, new AddGrantRequestDeserializer());
    module.addDeserializer(RevokeGrantRequest.class, new RevokeGrantRequestDeserializer());
    mapper.registerModule(module);
  }

  /**
   * Deserializer for {@link CreateCatalogRequest}. Backward compatible with the previous version of
   * the api
   */
  public static final class CreateCatalogRequestDeserializer
      extends JsonDeserializer<CreateCatalogRequest> {
    @Override
    public CreateCatalogRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("catalog")) {
        return CreateCatalogRequest.builder()
            .setCatalog(ctxt.readTreeAsValue((JsonNode) treeNode.get("catalog"), Catalog.class))
            .build();
      } else {
        return CreateCatalogRequest.builder()
            .setCatalog(ctxt.readTreeAsValue((JsonNode) treeNode, Catalog.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link CreatePrincipalRequest}. Backward compatible with the previous version
   * of the api
   */
  public static final class CreatePrincipalRequestDeserializer
      extends JsonDeserializer<CreatePrincipalRequest> {
    @Override
    public CreatePrincipalRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("principal")) {
        return CreatePrincipalRequest.builder()
            .setPrincipal(
                ctxt.readTreeAsValue((JsonNode) treeNode.get("principal"), Principal.class))
            .setCredentialRotationRequired(
                ctxt.readTreeAsValue(
                    (JsonNode) treeNode.get("credentialRotationRequired"), Boolean.class))
            .build();
      } else {
        return CreatePrincipalRequest.builder()
            .setPrincipal(ctxt.readTreeAsValue((JsonNode) treeNode, Principal.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link CreatePrincipalRoleRequest}. Backward compatible with the previous
   * version of the api
   */
  public static final class CreatePrincipalRoleRequestDeserializer
      extends JsonDeserializer<CreatePrincipalRoleRequest> {
    @Override
    public CreatePrincipalRoleRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("principalRole")) {
        return CreatePrincipalRoleRequest.builder()
            .setPrincipalRole(
                ctxt.readTreeAsValue((JsonNode) treeNode.get("principalRole"), PrincipalRole.class))
            .build();
      } else {
        return CreatePrincipalRoleRequest.builder()
            .setPrincipalRole(ctxt.readTreeAsValue((JsonNode) treeNode, PrincipalRole.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link GrantPrincipalRoleRequest}. Backward compatible with the previous
   * version of the api
   */
  public static final class GrantPrincipalRoleRequestDeserializer
      extends JsonDeserializer<GrantPrincipalRoleRequest> {
    @Override
    public GrantPrincipalRoleRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("principalRole")) {
        return GrantPrincipalRoleRequest.builder()
            .setPrincipalRole(
                ctxt.readTreeAsValue((JsonNode) treeNode.get("principalRole"), PrincipalRole.class))
            .build();
      } else {
        return GrantPrincipalRoleRequest.builder()
            .setPrincipalRole(ctxt.readTreeAsValue((JsonNode) treeNode, PrincipalRole.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link CreateCatalogRoleRequest} Backward compatible with the previous version
   * of the api
   */
  public static final class CreateCatalogRoleRequestDeserializer
      extends JsonDeserializer<CreateCatalogRoleRequest> {
    @Override
    public CreateCatalogRoleRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("catalogRole")) {
        return CreateCatalogRoleRequest.builder()
            .setCatalogRole(
                ctxt.readTreeAsValue((JsonNode) treeNode.get("catalogRole"), CatalogRole.class))
            .build();
      } else {
        return CreateCatalogRoleRequest.builder()
            .setCatalogRole(ctxt.readTreeAsValue((JsonNode) treeNode, CatalogRole.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link GrantCatalogRoleRequest} Backward compatible with the previous version
   * of the api
   */
  public static final class GrantCatalogRoleRequestDeserializer
      extends JsonDeserializer<GrantCatalogRoleRequest> {
    @Override
    public GrantCatalogRoleRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("catalogRole")) {
        return GrantCatalogRoleRequest.builder()
            .setCatalogRole(
                ctxt.readTreeAsValue((JsonNode) treeNode.get("catalogRole"), CatalogRole.class))
            .build();
      } else {
        return GrantCatalogRoleRequest.builder()
            .setCatalogRole(ctxt.readTreeAsValue((JsonNode) treeNode, CatalogRole.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link AddGrantRequest} Backward compatible with previous version of the api
   */
  public static final class AddGrantRequestDeserializer extends JsonDeserializer<AddGrantRequest> {
    @Override
    public AddGrantRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("grant")) {
        return AddGrantRequest.builder()
            .setGrant(ctxt.readTreeAsValue((JsonNode) treeNode.get("grant"), GrantResource.class))
            .build();
      } else {
        return AddGrantRequest.builder()
            .setGrant(ctxt.readTreeAsValue((JsonNode) treeNode, GrantResource.class))
            .build();
      }
    }
  }

  /**
   * Deserializer for {@link RevokeGrantRequest} Backward compatible with previous version of the
   * api
   */
  public static final class RevokeGrantRequestDeserializer
      extends JsonDeserializer<RevokeGrantRequest> {
    @Override
    public RevokeGrantRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      TreeNode treeNode = p.readValueAsTree();
      if (treeNode.isObject() && ((ObjectNode) treeNode).has("grant")) {
        return RevokeGrantRequest.builder()
            .setGrant(ctxt.readTreeAsValue((JsonNode) treeNode.get("grant"), GrantResource.class))
            .build();
      } else {
        return RevokeGrantRequest.builder()
            .setGrant(ctxt.readTreeAsValue((JsonNode) treeNode, GrantResource.class))
            .build();
      }
    }
  }
}
