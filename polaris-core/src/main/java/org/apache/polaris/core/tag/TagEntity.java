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
package org.apache.polaris.core.tag;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * A tag definition. Tags are children of a catalog, so a tag is identified by its name within the
 * catalog that contains it.
 *
 * <p>Like {@link org.apache.polaris.core.policy.PolicyEntity}, all tag-specific state lives in the
 * generic properties map rather than in dedicated columns, so a tag needs no persistence schema of
 * its own. The two list-valued properties are stored as JSON arrays.
 *
 * <p>Target types are held as plain strings rather than the API enum because this module does not
 * depend on the generated API types; converting to and from the wire enum is the service layer's
 * job. The service maps an unknown wire member to null during deserialization and rejects it in
 * validation.
 */
public class TagEntity extends PolarisEntity {

  public static final String TAG_COMMENT_KEY = "tag-comment";
  public static final String TAG_ALLOWED_VALUES_KEY = "tag-allowed-values";
  public static final String TAG_TARGET_TYPES_KEY = "tag-target-types";
  public static final String TAG_VERSION_KEY = "tag-version";

  private static final ObjectMapper MAPPER = JsonMapper.shared();

  TagEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.TAG, "Invalid entity type: %s", getType());
    Preconditions.checkState(
        getSubType() == PolarisEntitySubType.NULL_SUBTYPE,
        "Invalid entity sub type: %s",
        getSubType());
  }

  public static @Nullable TagEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new TagEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public @Nullable String getComment() {
    return getPropertiesAsMap().get(TAG_COMMENT_KEY);
  }

  @JsonIgnore
  public List<String> getAllowedValues() {
    String allowedValues = getPropertiesAsMap().get(TAG_ALLOWED_VALUES_KEY);
    Preconditions.checkNotNull(allowedValues, "Invalid tag entity: allowed values must exist");
    return readStringList(allowedValues, TAG_ALLOWED_VALUES_KEY);
  }

  @JsonIgnore
  public List<String> getTargetTypes() {
    String targetTypes = getPropertiesAsMap().get(TAG_TARGET_TYPES_KEY);
    Preconditions.checkNotNull(targetTypes, "Invalid tag entity: target types must exist");
    return readStringList(targetTypes, TAG_TARGET_TYPES_KEY);
  }

  @JsonIgnore
  public int getTagVersion() {
    String version = getPropertiesAsMap().get(TAG_VERSION_KEY);
    Preconditions.checkNotNull(version, "Invalid tag entity: version must exist");
    return Integer.parseInt(version);
  }

  private static List<String> readStringList(String json, String key) {
    try {
      return MAPPER.readValue(json, new TypeReference<List<String>>() {});
    } catch (Exception ex) {
      throw new IllegalStateException(
          String.format("Invalid tag entity: %s is not a JSON array of strings", key), ex);
    }
  }

  private static String writeStringList(List<String> values) {
    try {
      return MAPPER.writeValueAsString(values);
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to serialize tag property to json", ex);
    }
  }

  public static class Builder extends PolarisEntity.BaseBuilder<TagEntity, Builder> {
    public Builder(String tagName) {
      super();
      setType(PolarisEntityType.TAG);
      setName(tagName);
      setTagVersion(0);
    }

    public Builder(TagEntity original) {
      super(original);
    }

    @Override
    public TagEntity build() {
      Preconditions.checkArgument(
          properties.containsKey(TAG_ALLOWED_VALUES_KEY), "Allowed values must be specified");
      Preconditions.checkArgument(
          properties.containsKey(TAG_TARGET_TYPES_KEY), "Target types must be specified");
      return new TagEntity(buildBase());
    }

    /** A null comment leaves any existing comment untouched; an empty string clears it. */
    public Builder setComment(@Nullable String comment) {
      if (comment != null) {
        properties.put(TAG_COMMENT_KEY, comment);
      }
      return this;
    }

    public Builder setAllowedValues(List<String> allowedValues) {
      Preconditions.checkArgument(allowedValues != null, "Allowed values must be specified");
      properties.put(TAG_ALLOWED_VALUES_KEY, writeStringList(allowedValues));
      return this;
    }

    public Builder setTargetTypes(List<String> targetTypes) {
      Preconditions.checkArgument(targetTypes != null, "Target types must be specified");
      properties.put(TAG_TARGET_TYPES_KEY, writeStringList(targetTypes));
      return this;
    }

    public Builder setTagVersion(int version) {
      properties.put(TAG_VERSION_KEY, Integer.toString(version));
      return this;
    }
  }
}
