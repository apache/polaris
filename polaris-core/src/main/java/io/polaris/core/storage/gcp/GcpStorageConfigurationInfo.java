package io.polaris.core.storage.gcp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.polaris.core.storage.PolarisStorageConfigurationInfo;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Gcp storage storage configuration information. */
public class GcpStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 8 is an experimental result from generating GCP accessBoundaryRules when subscoping creds,
  // when the rule is too large, GCS only returns error: 400 bad request "Invalid arguments
  // provided in the request"
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 8;

  /** The gcp service account */
  @JsonProperty(value = "gcpServiceAccount", required = false)
  private @Nullable String gcpServiceAccount = null;

  @JsonCreator
  public GcpStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {
    super(StorageType.GCS, allowedLocations);
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.gcp.gcs.GCSFileIO";
  }

  public void setGcpServiceAccount(String gcpServiceAccount) {
    this.gcpServiceAccount = gcpServiceAccount;
  }

  public String getGcpServiceAccount() {
    return gcpServiceAccount;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", storageType)
        .add("allowedLocation", allowedLocations)
        .add("gcpServiceAccount", gcpServiceAccount)
        .toString();
  }
}
