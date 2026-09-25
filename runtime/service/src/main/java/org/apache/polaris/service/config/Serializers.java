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
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
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
import org.apache.polaris.core.tag.TagValidation;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;

public final class Serializers {
  private Serializers() {}

  public static void registerSerializers(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(TargetType.class, new TargetTypeDeserializer());
    module.addDeserializer(UpdateTagRequest.class, new UpdateTagRequestDeserializer());
    module.addDeserializer(CreateTagRequest.class, new CreateTagRequestDeserializer());
    module.addDeserializer(RenameTagRequest.class, new RenameTagRequestDeserializer());
    module.addDeserializer(AssignTagRequest.class, new AssignTagRequestDeserializer());
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
   * Deserializer for {@link TargetType}: maps the declared wire values and turns an unknown member
   * into null, so server-side validation rejects it with the documented error type instead of
   * surfacing a deserialization failure.
   */
  public static final class TargetTypeDeserializer extends JsonDeserializer<TargetType> {
    @Override
    public TargetType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String text = p.getValueAsString();
      for (TargetType targetType : TargetType.values()) {
        if (targetType.toString().equals(text)) {
          return targetType;
        }
      }
      return null;
    }
  }

  /**
   * Binds one field of a tag request, reporting a wrong JSON type as a schema failure that names
   * the field. Left to Jackson, a type mismatch escapes the body reader without an error envelope
   * at all, and the contract requires every error to carry one. An {@link IllegalArgumentException}
   * is answered 400 by the shared mapper, which builds that envelope, and the Tag error filter then
   * reports it as the shared schema-failure literal rather than a Tag validation error.
   */
  private static <T> T bindField(
      DeserializationContext ctxt, JsonNode value, String field, Class<T> type) {
    try {
      return ctxt.readTreeAsValue(value, type);
    } catch (IOException e) {
      throw new IllegalArgumentException("Field " + field + " has an invalid schema type", e);
    }
  }

  /** The list-valued counterpart of {@link #bindField}. */
  private static <T> List<T> bindList(
      DeserializationContext ctxt, JsonNode value, String field, Class<T> element) {
    try {
      return ctxt.readTreeAsValue(
          value, ctxt.getTypeFactory().constructCollectionType(List.class, element));
    } catch (IOException e) {
      throw new IllegalArgumentException("Field " + field + " has an invalid schema type", e);
    }
  }

  /**
   * Reads a list whose declared member type is a JSON string, refusing a member of any other type
   * instead of letting it be converted to one.
   *
   * <p>{@link #bindList} asks Jackson for a list of String, and Jackson converts a scalar member
   * rather than failing, exactly as it does for a single field: the number {@code 1} arrives as
   * {@code "1"}. A definition whose allowed values happen to look like numbers would then accept a
   * selection the client never sent, and no later check can tell the two apart, because the JSON
   * type is gone by the time a {@code List<String>} exists. This is the list counterpart of {@link
   * #readStringField}, and it reports the same schema failure.
   *
   * <p>{@code node} is the array itself. A null member is left alone: whether a value may be null
   * is a question about the value, not about its type, and the definition rules already answer it.
   */
  private static List<String> bindStringList(
      DeserializationContext ctxt, JsonNode node, String field) {
    if (!node.isArray()) {
      throw new IllegalArgumentException("Field " + field + " has an invalid schema type");
    }
    for (JsonNode member : node) {
      if (!member.isNull() && !member.isTextual()) {
        throw new IllegalArgumentException("Field " + field + " has an invalid schema type");
      }
    }
    return bindList(ctxt, node, field, String.class);
  }

  /**
   * Reads a field whose declared type is a JSON string, refusing any other JSON type instead of
   * letting it be converted to one.
   *
   * <p>{@link #bindField} asks Jackson for a String, and Jackson converts a scalar rather than
   * failing: a number arrives as its digits. For a version token that is the wrong answer twice
   * over, because the request is malformed and would instead be reported as a token that does not
   * match, which tells a client to reload a definition that was never the problem. An absent or
   * explicitly null field is left to the caller, which already says what each of those means.
   */
  private static String readStringField(
      DeserializationContext ctxt, ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isTextual()) {
      throw new IllegalArgumentException("Field " + field + " has an invalid schema type");
    }
    return bindField(ctxt, value, field, String.class);
  }

  /**
   * Deserializer for {@link UpdateTagRequest}: rejects a request that carries {@code target-types}.
   *
   * <p>target-types is create-only, so the update schema does not declare it. Unknown properties
   * are ignored service-wide, which would let both {@code "target-types": [...]} and an explicit
   * {@code "target-types": null} through unnoticed, and the spec requires 400 for either. Failing
   * on unknown properties instead would change every other schema's behavior, so the check is
   * scoped to this one request type and to that one key: any other unrecognized property is still
   * ignored.
   *
   * <p>The remaining fields are bound exactly as the generated model binds them, so an omitted
   * field and an explicit JSON null both arrive as null and mean "unchanged".
   */
  public static final class UpdateTagRequestDeserializer
      extends JsonDeserializer<UpdateTagRequest> {
    private static final String TARGET_TYPES = "target-types";

    @Override
    public UpdateTagRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      TreeNode treeNode = p.readValueAsTree();
      if (!treeNode.isObject()) {
        ctxt.reportInputMismatch(UpdateTagRequest.class, "Expected a JSON object");
      }
      ObjectNode node = (ObjectNode) treeNode;
      if (node.has(TARGET_TYPES)) {
        throw new BadRequestException("target-types is create-only and cannot be updated");
      }
      if (node.has("name")) {
        // Renaming is its own operation, so a name here is not a field this request can carry.
        // Saying so beats ignoring it: FAIL_ON_UNKNOWN_PROPERTIES is off for every Polaris request,
        // so silence would let a client believe a rename had been requested and accepted.
        throw new BadRequestException("name is not an updatable field; use the rename operation");
      }
      // An update states the whole editable definition, so both fields must be present. A present
      // description may be null, which clears it; an absent one is a missing required field, which
      // is a generic schema failure rather than a Tag validation error, so it takes the shared
      // literal through IllegalArgumentException rather than BadRequestException.
      if (!node.has("description")) {
        throw new IllegalArgumentException("Field description is required");
      }
      // description is declared a string, so a value of another JSON type is a malformed request
      // rather than text: reading it the same way as the version token keeps a number from being
      // stored as its own digits.
      //
      // An explicit null and an empty string are the two wire spellings of "no description", so the
      // null is folded into the empty string here. The fold has to happen before the request object
      // exists: the generated model marks this field required and rejects a null on sight, and that
      // check cannot see that the schema also declares it nullable. TagCatalog then turns the empty
      // string back into the one stored form, which is null.
      String description = readStringField(ctxt, node, "description");
      return new UpdateTagRequest(
          description == null ? "" : description,
          requireValues(ctxt, node),
          readStringField(ctxt, node, "current-tag-version"));
    }

    /**
     * Reads the values list an update must state. Absent and explicitly null are the same mistake
     * and get the same Tag validation answer, which is the literal the contract names for an
     * invalid definition values list.
     */
    private static List<String> requireValues(DeserializationContext ctxt, ObjectNode node)
        throws IOException {
      JsonNode value = node.get("values");
      if (value == null || value.isNull()) {
        throw new BadRequestException("values is required");
      }
      return bindStringList(ctxt, value, "values");
    }
  }

  /**
   * Deserializer for {@link CreateTagRequest}. The contract names a missing {@code values} or
   * {@code target-types}, and an invalid tag name, as Tag validation errors, which the wire reports
   * as {@code BadRequest}. Left to the generated model those cases are answered by the shared
   * required-property and constraint-violation paths instead, which report a generic schema
   * failure, and both of those run before any handler code. Reading the body here is therefore the
   * only place a Tag-specific answer is still possible.
   *
   * <p>An absent {@code name} is deliberately NOT answered here: the contract names invalid names,
   * not missing ones, so a missing name stays a generic schema failure through the model's own
   * required-property constraint.
   */
  public static final class CreateTagRequestDeserializer
      extends JsonDeserializer<CreateTagRequest> {
    @Override
    public CreateTagRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      TreeNode treeNode = p.readValueAsTree();
      if (!treeNode.isObject()) {
        ctxt.reportInputMismatch(CreateTagRequest.class, "Expected a JSON object");
      }
      ObjectNode node = (ObjectNode) treeNode;
      String name = readValue(ctxt, node, "name", String.class);
      TagValidation.validateName(name);
      // description is declared a string here too, so it is read the same way the update request
      // reads it: another JSON type is a malformed request rather than text to store.
      return new CreateTagRequest(
          name,
          readStringField(ctxt, node, "description"),
          requireValues(ctxt, node),
          requestedTargetTypes(ctxt, node));
    }

    private static <T> T readValue(
        DeserializationContext ctxt, ObjectNode node, String field, Class<T> type)
        throws IOException {
      JsonNode value = node.get(field);
      return value == null || value.isNull() ? null : bindField(ctxt, value, field, type);
    }

    /**
     * Reads the target kinds a create asks for. Omitting the key selects every kind this version
     * defines, and the set is stored and returned explicitly, so a definition created today does
     * not silently widen when a later version adds a kind. An explicit null is not the same as
     * omission: it names no kinds, which no definition can have.
     */
    private static List<TargetType> requestedTargetTypes(
        DeserializationContext ctxt, ObjectNode node) throws IOException {
      JsonNode value = node.get("target-types");
      if (value == null) {
        return List.of(TargetType.values());
      }
      if (value.isNull()) {
        throw new BadRequestException("target-types is required");
      }
      return bindList(ctxt, value, "target-types", TargetType.class);
    }

    /**
     * Reads the values list a create must state. Absent and explicitly null are the same mistake
     * and get the same Tag validation answer, which is the literal the contract names for an
     * invalid definition values list.
     */
    private static List<String> requireValues(DeserializationContext ctxt, ObjectNode node) {
      JsonNode value = node.get("values");
      if (value == null || value.isNull()) {
        throw new BadRequestException("values is required");
      }
      return bindStringList(ctxt, value, "values");
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

  /**
   * Reads a rename request, validating both names before the model's own constraints can answer.
   * The generated model declares the name pattern, and bean validation would report a violation of
   * it as a generic schema failure; the contract names a Tag validation error for an invalid name
   * on create or rename, and this is the only layer that runs first.
   */
  public static final class RenameTagRequestDeserializer
      extends JsonDeserializer<RenameTagRequest> {
    @Override
    public RenameTagRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      TreeNode treeNode = p.readValueAsTree();
      if (!treeNode.isObject()) {
        ctxt.reportInputMismatch(RenameTagRequest.class, "Expected a JSON object");
      }
      ObjectNode node = (ObjectNode) treeNode;
      String source = readValue(ctxt, node, "source", String.class);
      String destination = readValue(ctxt, node, "destination", String.class);
      TagValidation.validateName(source);
      TagValidation.validateName(destination);
      return new RenameTagRequest(
          source, destination, readStringField(ctxt, node, "current-tag-version"));
    }

    private static <T> T readValue(
        DeserializationContext ctxt, ObjectNode node, String field, Class<T> type)
        throws IOException {
      JsonNode value = node.get(field);
      return value == null || value.isNull() ? null : bindField(ctxt, value, field, type);
    }
  }

  /**
   * Deserializer for {@link AssignTagRequest}: refuses a selected-values member whose JSON type the
   * schema does not permit, instead of letting it be converted to text.
   *
   * <p>The generated model binds {@code values} as a list of String, and Jackson converts each
   * scalar on the way in, so a number member arrives as its digits and the operation then judges it
   * on content: it reports a selection the definition does not allow, or accepts one it does. A
   * definition whose allowed values contain that same text would therefore store an assignment for
   * a request the schema never permitted.
   *
   * <p>Registering this deserializer replaces the generated binding, so the two answers that
   * binding already gives are reproduced here rather than inherited: an absent field is the schema
   * failure its {@code required} creator property reports today, and an explicitly null field binds
   * to an empty list, which the operation answers as a selection that names no value.
   */
  public static final class AssignTagRequestDeserializer
      extends JsonDeserializer<AssignTagRequest> {
    @Override
    public AssignTagRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      TreeNode treeNode = p.readValueAsTree();
      if (!treeNode.isObject()) {
        ctxt.reportInputMismatch(AssignTagRequest.class, "Expected a JSON object");
      }
      ObjectNode node = (ObjectNode) treeNode;
      JsonNode values = node.get("values");
      if (values == null) {
        throw new IllegalArgumentException("Field values is required");
      }
      if (values.isNull()) {
        // An explicit null is not a schema failure here: the generated creator turns it into an
        // empty list and the operation answers it as a selection that names no value.
        return new AssignTagRequest(List.of());
      }
      return new AssignTagRequest(bindStringList(ctxt, values, "values"));
    }
  }
}
