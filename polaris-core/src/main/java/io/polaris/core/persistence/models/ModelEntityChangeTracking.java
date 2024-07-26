package io.polaris.core.persistence.models;

import io.polaris.core.entity.PolarisBaseEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * EntityChangeTracking model representing some attributes of a Polaris Entity. This is used to
 * exchange entity information with ENTITIES_CHANGE_TRACKING table
 */
@Entity
@Table(name = "ENTITIES_CHANGE_TRACKING")
public class ModelEntityChangeTracking {
  // the id of the catalog associated to that entity. NULL_ID if this entity is top-level like
  // a catalog
  @Id private long catalogId;

  // the id of the entity which was resolved
  @Id private long id;

  // the version that this entity had when it was resolved
  private int entityVersion;

  // current version for that entity, will be monotonically incremented
  private int grantRecordsVersion;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

  public ModelEntityChangeTracking() {}

  public ModelEntityChangeTracking(PolarisBaseEntity entity) {
    this.catalogId = entity.getCatalogId();
    this.id = entity.getId();
    this.entityVersion = entity.getEntityVersion();
    this.grantRecordsVersion = entity.getGrantRecordsVersion();
  }

  public long getCatalogId() {
    return catalogId;
  }

  public long getId() {
    return id;
  }

  public int getEntityVersion() {
    return entityVersion;
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public void update(PolarisBaseEntity entity) {
    this.entityVersion = entity.getEntityVersion();
    this.grantRecordsVersion = entity.getGrantRecordsVersion();
  }
}
