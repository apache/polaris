package io.polaris.core.storage.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.polaris.core.storage.PolarisStorageConfigurationInfo;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Azure storage configuration information. */
public class AzureStorageConfigurationInfo extends PolarisStorageConfigurationInfo {
  // technically there is no limitation since expectation for Azure locations are for the same
  // storage account and same container
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 20;

  // Azure tenant id
  private final @NotNull String tenantId;

  /** The multi tenant app name for the service principal */
  @JsonProperty(value = "multiTenantAppName", required = false)
  private @Nullable String multiTenantAppName = null;

  /** The consent url to the Azure permissions request page */
  @JsonProperty(value = "consentUrl", required = false)
  private @Nullable String consentUrl = null;

  @JsonCreator
  public AzureStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations,
      @JsonProperty(value = "tenantId", required = true) @NotNull String tenantId) {
    super(StorageType.AZURE, allowedLocations);
    this.tenantId = tenantId;
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
  }

  public @NotNull String getTenantId() {
    return tenantId;
  }

  public String getMultiTenantAppName() {
    return multiTenantAppName;
  }

  public void setMultiTenantAppName(String multiTenantAppName) {
    this.multiTenantAppName = multiTenantAppName;
  }

  public String getConsentUrl() {
    return consentUrl;
  }

  public void setConsentUrl(String consentUrl) {
    this.consentUrl = consentUrl;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", storageType)
        .add("tenantId", tenantId)
        .add("allowedLocation", allowedLocations)
        .add("multiTenantAppName", multiTenantAppName)
        .add("consentUrl", consentUrl)
        .toString();
  }

  @Override
  public void validatePrefixForStorageType() {
    this.allowedLocations.forEach(AzureLocation::new);
  }
}
