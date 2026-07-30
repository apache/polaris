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
package org.apache.polaris.core.semantic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.jspecify.annotations.Nullable;

/**
 * A Polaris entity that stores an Apache Ossie semantic-model document.
 *
 * <p>Following the precedent set by {@link org.apache.polaris.core.policy.PolicyEntity}, the full
 * document body is stored inside the entity {@code properties} map: the declared Ossie spec version
 * under {@link #SPEC_VERSION_KEY} and the Ossie document (as a JSON string) under {@link
 * #CONTENT_KEY}.
 */
public class SemanticModelEntity extends PolarisEntity {

  /** The declared Ossie spec version of the stored document (e.g. {@code 0.1.1}). */
  public static final String SPEC_VERSION_KEY = "semantic-model.spec-version";

  /** The Ossie document serialized as a JSON string. */
  public static final String CONTENT_KEY = "semantic-model.content";

  public SemanticModelEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.SEMANTIC_MODEL, "Invalid entity type: %s", getType());
    Preconditions.checkState(
        getSubType() == PolarisEntitySubType.NULL_SUBTYPE,
        "Invalid entity sub type: %s",
        getSubType());
  }

  public static @Nullable SemanticModelEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new SemanticModelEntity(sourceEntity);
    }
    return null;
  }

  /** The declared Ossie spec version of the stored document. */
  @JsonIgnore
  public String getSpecVersion() {
    return getPropertiesAsMap().get(SPEC_VERSION_KEY);
  }

  /** The Ossie document body, serialized as a JSON string. */
  @JsonIgnore
  public String getContent() {
    return getPropertiesAsMap().get(CONTENT_KEY);
  }

  @JsonIgnore
  public @Nullable Namespace getParentNamespace() {
    String parentNamespace = getInternalPropertiesAsMap().get(NamespaceEntity.PARENT_NAMESPACE_KEY);
    if (parentNamespace != null) {
      return PolarisEntityUtils.decodeNamespace(parentNamespace);
    }
    return null;
  }

  public static class Builder extends PolarisEntity.BaseBuilder<SemanticModelEntity, Builder> {
    public Builder(Namespace namespace, String modelName) {
      super();
      setType(PolarisEntityType.SEMANTIC_MODEL);
      setParentNamespace(namespace);
      setName(modelName);
    }

    public Builder(SemanticModelEntity original) {
      super(original);
    }

    @Override
    public SemanticModelEntity build() {
      Preconditions.checkArgument(
          properties.containsKey(SPEC_VERSION_KEY),
          "Semantic model spec version must be specified");
      Preconditions.checkArgument(
          properties.containsKey(CONTENT_KEY), "Semantic model content must be specified");
      return new SemanticModelEntity(buildBase());
    }

    public Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, PolarisEntityUtils.encodeNamespace(namespace));
      }
      return this;
    }

    public Builder setSpecVersion(String specVersion) {
      properties.put(SPEC_VERSION_KEY, specVersion);
      return this;
    }

    public Builder setContent(String content) {
      properties.put(CONTENT_KEY, content);
      return this;
    }
  }
}
