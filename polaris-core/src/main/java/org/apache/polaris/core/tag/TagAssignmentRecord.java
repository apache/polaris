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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * A single tag assignment: one selected value of one tag definition on one target. The identity of
 * an assignment is the tuple (targetCatalogId, targetId, fieldId, tagCatalogId, tagId); the
 * selected value is content, not identity, so writing an assignment with the same identity replaces
 * the stored value.
 *
 * <p>fieldId is 0 when the whole target entity is tagged and a top-level Iceberg field id when a
 * column of a table is tagged (Iceberg assigns field ids starting at 1, so 0 can never collide with
 * a real column).
 */
public class TagAssignmentRecord {
  // canonical containing-catalog id of the target: a catalog target stores its own id, every
  // other target stores the id of the catalog containing it
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

  public TagAssignmentRecord() {}

  /**
   * The canonical containing-catalog id stored for a target: a catalog target stores its own id,
   * every other target stores the id of the catalog containing it. Every write, read, and delete
   * keyed by (targetCatalogId, targetId) must derive the pair through this method.
   */
  public static long containingCatalogId(PolarisEntityCore target) {
    return target.getTypeCode() == PolarisEntityType.CATALOG.getCode()
        ? target.getId()
        : target.getCatalogId();
  }

  public long getTargetCatalogId() {
    return targetCatalogId;
  }

  public void setTargetCatalogId(long targetCatalogId) {
    this.targetCatalogId = targetCatalogId;
  }

  public long getTargetId() {
    return targetId;
  }

  public void setTargetId(long targetId) {
    this.targetId = targetId;
  }

  public int getFieldId() {
    return fieldId;
  }

  public void setFieldId(int fieldId) {
    this.fieldId = fieldId;
  }

  public long getTagCatalogId() {
    return tagCatalogId;
  }

  public void setTagCatalogId(long tagCatalogId) {
    this.tagCatalogId = tagCatalogId;
  }

  public long getTagId() {
    return tagId;
  }

  public void setTagId(long tagId) {
    this.tagId = tagId;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  /**
   * Constructor
   *
   * @param targetCatalogId id of the catalog where the target entity resides
   * @param targetId id of the target entity; for a column assignment, the containing table
   * @param fieldId 0 for a whole-object assignment, else the top-level Iceberg field id
   * @param tagCatalogId id of the catalog where the tag definition resides
   * @param tagId id of the tag definition
   * @param value the selected value
   */
  @JsonCreator
  public TagAssignmentRecord(
      @JsonProperty("targetCatalogId") long targetCatalogId,
      @JsonProperty("targetId") long targetId,
      @JsonProperty("fieldId") int fieldId,
      @JsonProperty("tagCatalogId") long tagCatalogId,
      @JsonProperty("tagId") long tagId,
      @JsonProperty("value") String value) {
    this.targetCatalogId = targetCatalogId;
    this.targetId = targetId;
    this.fieldId = fieldId;
    this.tagCatalogId = tagCatalogId;
    this.tagId = tagId;
    this.value = value;
  }

  /**
   * Copy constructor
   *
   * @param record tag assignment record to copy
   */
  public TagAssignmentRecord(TagAssignmentRecord record) {
    this.targetCatalogId = record.getTargetCatalogId();
    this.targetId = record.getTargetId();
    this.fieldId = record.getFieldId();
    this.tagCatalogId = record.getTagCatalogId();
    this.tagId = record.getTagId();
    this.value = record.getValue();
  }

  @Override
  public String toString() {
    return "TagAssignmentRecord{"
        + "targetCatalogId="
        + targetCatalogId
        + ", targetId="
        + targetId
        + ", fieldId="
        + fieldId
        + ", tagCatalogId="
        + tagCatalogId
        + ", tagId="
        + tagId
        + ", value='"
        + value
        + "'}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TagAssignmentRecord that)) return false;
    return targetCatalogId == that.targetCatalogId
        && targetId == that.targetId
        && fieldId == that.fieldId
        && tagCatalogId == that.tagCatalogId
        && tagId == that.tagId
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetCatalogId, targetId, fieldId, tagCatalogId, tagId, value);
  }
}
