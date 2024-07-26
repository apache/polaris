package io.polaris.core.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Support for file:// URLs in storage configuration. This is pretty-much only used for testing.
 * Supports URLs that start with file:// or /, but also supports wildcard (*) to support certain
 * test cases.
 */
public class FileStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public FileStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {
    super(StorageType.FILE, allowedLocations);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.hadoop.HadoopFileIO";
  }

  @Override
  public void validatePrefixForStorageType() {
    this.allowedLocations.forEach(
        loc -> {
          if (!loc.startsWith(storageType.getPrefix())
              && !loc.startsWith("/")
              && !loc.equals("*")) {
            throw new IllegalArgumentException(
                String.format(
                    "Location prefix not allowed: '%s', expected prefix: file:// or / or *", loc));
          }
        });
  }
}
