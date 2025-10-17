package org.apache.polaris.service.events.json.mixins;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.json.serde.TableIdentifierToStringSerializer;

@JsonTypeName("AfterRefreshTableEvent")
public abstract class AfterRefreshTableEventMixin {
    @JsonProperty("catalog_name")
    abstract String catalogName();

    @JsonProperty("table_identifier")
    @JsonSerialize(using = TableIdentifierToStringSerializer.class)
    abstract TableIdentifier tableIdentifier();
}