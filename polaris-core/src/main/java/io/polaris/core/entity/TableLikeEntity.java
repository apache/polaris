package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

public class TableLikeEntity extends PolarisEntity {
  // For applicable types, this key on the "internalProperties" map will return the location
  // of the internalProperties JSON file.
  public static final String METADATA_LOCATION_KEY = "metadata-location";

  public TableLikeEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static TableLikeEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new TableLikeEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public TableIdentifier getTableIdentifier() {
    Namespace parent = getParentNamespace();
    return TableIdentifier.of(parent, getName());
  }

  @JsonIgnore
  public Namespace getParentNamespace() {
    String encodedNamespace =
        getInternalPropertiesAsMap().get(NamespaceEntity.PARENT_NAMESPACE_KEY);
    if (encodedNamespace == null) {
      return Namespace.empty();
    }
    return RESTUtil.decodeNamespace(encodedNamespace);
  }

  @JsonIgnore
  public String getMetadataLocation() {
    return getInternalPropertiesAsMap().get(METADATA_LOCATION_KEY);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<TableLikeEntity, Builder> {
    public Builder(TableIdentifier identifier, String metadataLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
      setTableIdentifier(identifier);
      setMetadataLocation(metadataLocation);
    }

    public Builder(TableLikeEntity original) {
      super(original);
    }

    public TableLikeEntity build() {
      return new TableLikeEntity(buildBase());
    }

    public Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public Builder setMetadataLocation(String location) {
      internalProperties.put(METADATA_LOCATION_KEY, location);
      return this;
    }
  }
}
