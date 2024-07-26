package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Simple class to represent the version and grant records version associated to an entity */
public class PolarisChangeTrackingVersions {
  // entity version
  private final int entityVersion;

  // entity grant records version
  private final int grantRecordsVersion;

  /**
   * Constructor
   *
   * @param entityVersion entity version
   * @param grantRecordsVersion entity grant records version
   */
  @JsonCreator
  public PolarisChangeTrackingVersions(
      @JsonProperty("entityVersion") int entityVersion,
      @JsonProperty("grantRecordsVersion") int grantRecordsVersion) {
    this.entityVersion = entityVersion;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  public int getEntityVersion() {
    return entityVersion;
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  @Override
  public String toString() {
    return "PolarisChangeTrackingVersions{"
        + "entityVersion="
        + entityVersion
        + ", grantRecordsVersion="
        + grantRecordsVersion
        + '}';
  }
}
