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
package org.apache.polaris.persistence.relational.jdbc.models;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

public class ModelTagAssignmentRecord implements Converter<TagAssignmentRecord> {
  public static final String TABLE_NAME = "TAG_ASSIGNMENT_RECORD";

  // the selected value is stored in "tag_value": "value" is a reserved word in H2
  public static final List<String> ALL_COLUMNS =
      List.of(
          "target_catalog_id", "target_id", "field_id", "tag_catalog_id", "tag_id", "tag_value");

  // id of the catalog where the target entity resides
  private long targetCatalogId;

  // id of the target entity; for a column assignment, the containing table
  private long targetId;

  // 0 for a whole-object assignment; the top-level Iceberg field id for a column assignment
  private int fieldId;

  // id of the catalog where the tag definition resides
  private long tagCatalogId;

  // id of the tag definition
  private long tagId;

  // the selected value
  private String value;

  public long getTargetCatalogId() {
    return targetCatalogId;
  }

  public long getTargetId() {
    return targetId;
  }

  public int getFieldId() {
    return fieldId;
  }

  public long getTagCatalogId() {
    return tagCatalogId;
  }

  public long getTagId() {
    return tagId;
  }

  public String getValue() {
    return value;
  }

  public static ModelTagAssignmentRecord.Builder builder() {
    return new ModelTagAssignmentRecord.Builder();
  }

  public static final class Builder {
    private final ModelTagAssignmentRecord record;

    private Builder() {
      record = new ModelTagAssignmentRecord();
    }

    public Builder targetCatalogId(long targetCatalogId) {
      record.targetCatalogId = targetCatalogId;
      return this;
    }

    public Builder targetId(long targetId) {
      record.targetId = targetId;
      return this;
    }

    public Builder fieldId(int fieldId) {
      record.fieldId = fieldId;
      return this;
    }

    public Builder tagCatalogId(long tagCatalogId) {
      record.tagCatalogId = tagCatalogId;
      return this;
    }

    public Builder tagId(long tagId) {
      record.tagId = tagId;
      return this;
    }

    public Builder value(String value) {
      record.value = value;
      return this;
    }

    public ModelTagAssignmentRecord build() {
      return record;
    }
  }

  public static ModelTagAssignmentRecord fromTagAssignmentRecord(TagAssignmentRecord record) {
    if (record == null) return null;

    return ModelTagAssignmentRecord.builder()
        .targetCatalogId(record.getTargetCatalogId())
        .targetId(record.getTargetId())
        .fieldId(record.getFieldId())
        .tagCatalogId(record.getTagCatalogId())
        .tagId(record.getTagId())
        .value(record.getValue())
        .build();
  }

  public static TagAssignmentRecord toTagAssignmentRecord(ModelTagAssignmentRecord model) {
    if (model == null) return null;

    return new TagAssignmentRecord(
        model.getTargetCatalogId(),
        model.getTargetId(),
        model.getFieldId(),
        model.getTagCatalogId(),
        model.getTagId(),
        model.getValue());
  }

  @Override
  public TagAssignmentRecord fromResultSet(ResultSet rs) throws SQLException {
    var model =
        ModelTagAssignmentRecord.builder()
            .targetCatalogId(rs.getLong("target_catalog_id"))
            .targetId(rs.getLong("target_id"))
            .fieldId(rs.getInt("field_id"))
            .tagCatalogId(rs.getLong("tag_catalog_id"))
            .tagId(rs.getLong("tag_id"))
            .value(rs.getString("tag_value"))
            .build();

    return toTagAssignmentRecord(model);
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("target_catalog_id", targetCatalogId);
    map.put("target_id", targetId);
    map.put("field_id", fieldId);
    map.put("tag_catalog_id", tagCatalogId);
    map.put("tag_id", tagId);
    map.put("tag_value", value);
    return map;
  }
}
