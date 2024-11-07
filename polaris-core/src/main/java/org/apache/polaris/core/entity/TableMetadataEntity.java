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
package org.apache.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TableMetadataEntity extends PolarisEntity {
    private static String CONTENT_KEY = "content";
    private static String METADATA_LOCATION_KEY = "metadata_location";

    public TableMetadataEntity(PolarisBaseEntity sourceEntity) {
        super(sourceEntity);
    }

    public static TableMetadataEntity of(PolarisBaseEntity sourceEntity) {
        if (sourceEntity != null) {
            return new TableMetadataEntity(sourceEntity);
        }
        return null;
    }

    @JsonIgnore
    public String getContent() {
        return getInternalPropertiesAsMap().get(CONTENT_KEY);
    }

    @JsonIgnore
    public String getMetadataLocation() {
        return getInternalPropertiesAsMap().get(METADATA_LOCATION_KEY);
    }

    public static class Builder extends PolarisEntity.BaseBuilder<TableMetadataEntity, TableMetadataEntity.Builder> {
        public Builder(String metadataLocation, String content) {
            super();
            setName(metadataLocation);
            setType(PolarisEntityType.TABLE_METADATA);
            setMetadataLocation(metadataLocation);
            setContent(content);
        }

        @Override
        public TableMetadataEntity build() {
            return new TableMetadataEntity(buildBase());
        }

        public TableMetadataEntity.Builder setContent(String content) {
            this.internalProperties.put(CONTENT_KEY, content);
            return this;
        }

        public TableMetadataEntity.Builder setMetadataLocation(String metadataLocation) {
            this.internalProperties.put(METADATA_LOCATION_KEY, metadataLocation);
            return this;
        }
    }
}
