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

package org.apache.polaris.service.events.listeners;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.EventPayloadPruner;

@ApplicationScoped
@DefaultBean
class DefaultEventPayloadPruner implements EventPayloadPruner {

  @Override
  public Map<String, String> prune(AttributeKey<?> key, Object value) {
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return Map.of(key.name(), value.toString());
    }

    if (value instanceof Namespace namespace) {
      return Map.of(key.name(), namespace.toString());
    }

    if (value instanceof TableIdentifier tableIdentifier) {
      return Map.of(key.name(), tableIdentifier.toString());
    }

    if (key.equals(EventAttributes.TABLE_METADATA) && value instanceof TableMetadata metadata) {
      return pruneTableMetadata(metadata);
    }

    if (key.equals(EventAttributes.LOAD_TABLE_RESPONSE)
        && value instanceof LoadTableResponse response) {
      return pruneTableMetadata(response.tableMetadata());
    }

    if (key.equals(EventAttributes.RENAME_TABLE_REQUEST)
        && value instanceof RenameTableRequest request) {
      Map<String, String> result = new LinkedHashMap<>();
      result.put("rename_source", request.source().toString());
      result.put("rename_destination", request.destination().toString());
      return result;
    }

    return Map.of(key.name(), value.toString());
  }

  private static Map<String, String> pruneTableMetadata(TableMetadata metadata) {
    if (metadata == null) {
      return Map.of();
    }
    Map<String, String> summary = new LinkedHashMap<>();
    if (metadata.uuid() != null) {
      summary.put("table_uuid", metadata.uuid());
    }
    summary.put("table_location", metadata.location());
    summary.put("table_format_version", String.valueOf(metadata.formatVersion()));
    summary.put(
        "table_current_snapshot_id",
        String.valueOf(
            metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1));
    summary.put("table_schema", metadata.schema().toString());
    summary.put("table_last_updated_ms", String.valueOf(metadata.lastUpdatedMillis()));
    return summary;
  }
}
