package org.apache.polaris.service.catalog.conversion.xtable.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ConvertedTable {
  @JsonProperty("target-format")
  private String targetFormat;

  @JsonProperty("target-metadata-path")
  private String targetMetadataPath;

  @JsonProperty("target-schema")
  private String targetSchema;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ConvertedTable(
      @JsonProperty("target-format") String targetFormat,
      @JsonProperty("target-metadata-path") String targetMetadataPath,
      @JsonProperty("target-schema") String targetSchema) {
    this.targetFormat = targetFormat;
    this.targetMetadataPath = targetMetadataPath;
    this.targetSchema = targetSchema;
  }
}
