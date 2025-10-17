package org.apache.polaris.service.events.json.mixins;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Mixins for Iceberg classes we don't control, to keep JSON concise.
 * The @JsonValue marks toString() as the value to serialize.
 */
public class IcebergThirdPartyMixins {
    private IcebergThirdPartyMixins() {}

    public abstract static class NamespaceMixin {
        @Override
        @JsonValue
        public abstract String toString(); // serializes "namespace" as "db.sales"
    }

    public abstract static class TableIdentifierMixin {
        @Override
        @JsonValue
        public abstract String toString(); // serializes "table_identifier" as "db.sales.orders"
    }

}
