package io.polaris.core.persistence;

import com.google.common.collect.ImmutableList;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisGrantRecord;
import io.polaris.core.persistence.cache.EntityCacheEntry;
import java.util.List;

public class ResolvedPolarisEntity {
  private final PolarisEntity entity;

  // only non-empty if this entity can be a grantee; these are the grants on other
  // roles/securables granted to this entity.
  private final List<PolarisGrantRecord> grantRecordsAsGrantee;

  // grants associated to this entity as the securable; for a principal role or catalog role
  // these may be ROLE_USAGE or other permission-management privileges. For a catalog securable,
  // these are the grants like TABLE_READ_PROPERTIES, NAMESPACE_LIST, etc.
  private final List<PolarisGrantRecord> grantRecordsAsSecurable;

  public ResolvedPolarisEntity(
      PolarisEntity entity,
      List<PolarisGrantRecord> grantRecordsAsGrantee,
      List<PolarisGrantRecord> grantRecordsAsSecurable) {
    this.entity = entity;
    // TODO: Precondition checks that grantee or securable ids in grant records match entity as
    // expected.
    this.grantRecordsAsGrantee = grantRecordsAsGrantee;
    this.grantRecordsAsSecurable = grantRecordsAsSecurable;
  }

  public ResolvedPolarisEntity(EntityCacheEntry cacheEntry) {
    this.entity = PolarisEntity.of(cacheEntry.getEntity());
    this.grantRecordsAsGrantee = ImmutableList.copyOf(cacheEntry.getGrantRecordsAsGrantee());
    this.grantRecordsAsSecurable = ImmutableList.copyOf(cacheEntry.getGrantRecordsAsSecurable());
  }

  public PolarisEntity getEntity() {
    return entity;
  }

  /** The grant records associated with this entity being the grantee of the record. */
  public List<PolarisGrantRecord> getGrantRecordsAsGrantee() {
    return grantRecordsAsGrantee;
  }

  /** The grant records associated with this entity being the securable of the record. */
  public List<PolarisGrantRecord> getGrantRecordsAsSecurable() {
    return grantRecordsAsSecurable;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("entity:");
    sb.append(entity);
    sb.append(";grantRecordsAsGrantee:");
    sb.append(grantRecordsAsGrantee);
    sb.append(";grantRecordsAsSecurable:");
    sb.append(grantRecordsAsSecurable);
    return sb.toString();
  }
}
