package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Simple record like class to represent the unique identifier of an entity */
public class PolarisEntityId {

  // id of the catalog for this entity. If this entity is top-level, this will be NULL. Only not
  // null if this entity is a catalog entity like a namespace, a role, a table, a view, ...
  private final long catalogId;

  // entity id
  private final long id;

  @JsonCreator
  public PolarisEntityId(@JsonProperty("catalogId") long catalogId, @JsonProperty("id") long id) {
    this.catalogId = catalogId;
    this.id = id;
  }

  public long getCatalogId() {
    return catalogId;
  }

  public long getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PolarisEntityId that = (PolarisEntityId) o;
    return catalogId == that.catalogId && id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogId, id);
  }

  @Override
  public String toString() {
    return "PolarisEntityId{" + "catalogId=" + catalogId + ", id=" + id + '}';
  }
}
