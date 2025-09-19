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
package org.apache.polaris.service.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;
import org.apache.iceberg.TableMetadata;

public class TableUpdateNotification {

  private String tableName;
  private Long timestamp;
  private String tableUuid;
  private String metadataLocation;
  private TableMetadata metadata;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("table-name")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("timestamp")
  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("table-uuid")
  public String getTableUuid() {
    return tableUuid;
  }

  public void setTableUuid(String tableUuid) {
    this.tableUuid = tableUuid;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("metadata-location")
  public String getMetadataLocation() {
    return metadataLocation;
  }

  public void setMetadataLocation(String metadataLocation) {
    this.metadataLocation = metadataLocation;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("metadata")
  public TableMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(TableMetadata metadata) {
    this.metadata = metadata;
  }

  public TableUpdateNotification() {}

  public TableUpdateNotification(
      String tableName,
      Long timestamp,
      String tableUuid,
      String metadataLocation,
      TableMetadata metadata) {
    this.tableName = tableName;
    this.timestamp = timestamp;
    this.tableUuid = tableUuid;
    this.metadataLocation = metadataLocation;
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableUpdateNotification)) {
      return false;
    }
    TableUpdateNotification tableUpdateNotification = (TableUpdateNotification) o;
    return Objects.equals(this.tableName, tableUpdateNotification.tableName)
        && Objects.equals(this.timestamp, tableUpdateNotification.timestamp)
        && Objects.equals(this.tableUuid, tableUpdateNotification.tableUuid)
        && Objects.equals(this.metadataLocation, tableUpdateNotification.metadataLocation)
        && Objects.equals(this.metadata, tableUpdateNotification.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, timestamp, tableUuid, metadataLocation, metadata);
  }

  @Override
  public String toString() {
    return "class TableUpdateNotification {\n"
        + "    tableName: "
        + toIndentedString(tableName)
        + "\n"
        + "    timestamp: "
        + toIndentedString(timestamp)
        + "\n"
        + "    tableUuid: "
        + toIndentedString(tableUuid)
        + "\n"
        + "    metadataLocation: "
        + toIndentedString(metadataLocation)
        + "\n"
        + "    metadata: "
        + toIndentedString(metadata)
        + "\n"
        + "}";
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private static String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private String tableName;
    private Long timestamp;
    private String tableUuid;
    private String metadataLocation;
    private TableMetadata metadata;

    private Builder() {}

    public final Builder tableName(String tableName) {
      if (tableName == null) {
        throw new IllegalArgumentException("Null table name supplied");
      }
      this.tableName = tableName;
      return this;
    }

    public final Builder timestamp(Long timestamp) {
      if (timestamp == null) {
        throw new IllegalArgumentException("timestamp can't be null");
      }
      this.timestamp = timestamp;
      return this;
    }

    public final Builder metadataLocation(String metadataLocation) {
      if (metadataLocation == null) {
        throw new IllegalArgumentException("metadataLocation can't be null");
      }
      this.metadataLocation = metadataLocation;
      return this;
    }

    public final Builder metadata(TableMetadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public final Builder tableUuid(String tableUuid) {
      if (tableUuid == null) {
        throw new IllegalArgumentException("timestamp can't be null");
      }
      this.tableUuid = tableUuid;
      return this;
    }

    public TableUpdateNotification build() {

      return new TableUpdateNotification(
          tableName, timestamp, tableUuid, metadataLocation, metadata);
    }
  }
}
