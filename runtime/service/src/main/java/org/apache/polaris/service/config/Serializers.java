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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.util.Locale;
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
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;

public final class Serializers {
  private Serializers() {}

  public static void registerSerializers(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    // Add case-insensitive storage type enum deserializer
    module.addDeserializer(
        StorageConfigInfo.StorageTypeEnum.class, new StorageTypeEnumDeserializer());
    // Polymorphic subtype resolution is handled via registered aliases; enum is case-insensitive.
    // No need to wrap bean deserializers for StorageConfigInfo.
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

    // Intercept unknown type ids for StorageConfigInfo and try case-normalizing the id to
    // uppercase before failing, so we don't need to register all case combinations.
    mapper.addHandler(new CaseInsensitiveTypeIdHandler());
  }

  /**
   * DeserializationProblemHandler that uppercases unknown type ids for StorageConfigInfo to allow
   * case-insensitive discriminator values without enumerating all variants.
   */
  public static final class CaseInsensitiveTypeIdHandler
      extends com.fasterxml.jackson.databind.deser.DeserializationProblemHandler {
    @Override
    public com.fasterxml.jackson.databind.JavaType handleUnknownTypeId(
        DeserializationContext ctxt,
        com.fasterxml.jackson.databind.JavaType baseType,
        String subTypeId,
        TypeIdResolver idResolver,
        String failureMsg)
        throws IOException {
      if (StorageConfigInfo.class.isAssignableFrom(baseType.getRawClass()) && subTypeId != null) {
        String upper = subTypeId.toUpperCase(Locale.ROOT);
        com.fasterxml.jackson.databind.JavaType t = idResolver.typeFromId(ctxt, upper);
        if (t != null) {
          return t;
        }
        // Provide a clear, consistent error when the type id is unrecognized
        throw new JsonMappingException(
            ctxt.getParser(),
            String.format(
                "Invalid storage type '%s'. Valid values are: S3, GCS, AZURE, FILE",
                subTypeId));
      }
      return null; // fall back to default handling for other types
    }
  }

  /**
   * BeanDeserializerModifier that wraps StorageConfigInfo deserializers to normalize the
   * storageType field to uppercase before deserialization, enabling case-insensitive storage type
   * names.
   */
  public static final class StorageTypeDeserializerModifier extends BeanDeserializerModifier {

    @Override
    public JsonDeserializer<?> modifyDeserializer(
        DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
      // Only wrap the deserializer for StorageConfigInfo and its subclasses
      if (StorageConfigInfo.class.isAssignableFrom(beanDesc.getBeanClass())) {
        return new StorageTypeNormalizingDeserializer(deserializer);
      }
      return deserializer;
    }
  }

  /**
   * Wrapper deserializer that normalizes the storageType field to uppercase before delegating to
   * the default deserializer.
   */
  public static final class StorageTypeNormalizingDeserializer extends JsonDeserializer<Object> {

    private final JsonDeserializer<?> delegate;

    public StorageTypeNormalizingDeserializer(JsonDeserializer<?> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      // Read tokens into a buffer while normalizing storageType
      TokenBuffer buffer = new TokenBuffer(p);
      boolean inObject = false;
      String lastFieldName = null;

      // Copy all tokens, normalizing storageType values
      while (p.nextToken() != null) {
        JsonToken token = p.currentToken();

        if (token == JsonToken.START_OBJECT) {
          inObject = true;
          buffer.copyCurrentEvent(p);
        } else if (token == JsonToken.END_OBJECT) {
          inObject = false;
          buffer.copyCurrentEvent(p);
          break; // End of this object
        } else if (token == JsonToken.FIELD_NAME) {
          lastFieldName = p.getCurrentName();
          buffer.copyCurrentEvent(p);
        } else if (token == JsonToken.VALUE_STRING
            && "storageType".equals(lastFieldName)
            && inObject) {
          // Normalize storageType value to uppercase
          String value = p.getText();
          buffer.writeString(value.toUpperCase(Locale.ROOT));
        } else {
          buffer.copyCurrentEvent(p);
        }
      }

      // Create a new parser from the buffered tokens
      JsonParser newParser = buffer.asParser();
      newParser.nextToken(); // Position to first token

      // Delegate to the original deserializer with the modified parser
      return delegate.deserialize(newParser, ctxt);
    }
  }

  /**
   * Deserializer for {@link StorageConfigInfo.StorageTypeEnum} that accepts case-insensitive input.
   * Normalizes all input to uppercase to match enum values.
   */
  public static final class StorageTypeEnumDeserializer
      extends JsonDeserializer<StorageConfigInfo.StorageTypeEnum> {

    @Override
    public StorageConfigInfo.StorageTypeEnum deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String value = p.getText();
      if (value == null) {
        return null;
      }

      // Convert to uppercase to handle case-insensitive input
      String upperValue = value.toUpperCase(Locale.ROOT);

      try {
        return StorageConfigInfo.StorageTypeEnum.valueOf(upperValue);
      } catch (IllegalArgumentException e) {
        // Provide clear error message with valid options
        throw new JsonMappingException(
            p,
            String.format(
                "Invalid storage type '%s'. Valid values are: S3, GCS, AZURE, FILE (case-insensitive)",
                value));
      }
    }
  }

  /**
   * Deserializer for {@link CreateCatalogRequest}. Backward compatible with the previous version of
   * the api
   */
  public static final class CreateCatalogRequestDeserializer
      extends JsonDeserializer<CreateCatalogRequest> {
    @Override
    public CreateCatalogRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
        throws IOException {
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
