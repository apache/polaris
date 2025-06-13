package org.apache.polaris.service.catalog.conversion.xtable.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ConvertTableResponse {

  @JsonProperty("convertedTables")
  private final List<ConvertedTable> convertedTables;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ConvertTableResponse(
      @JsonProperty("convertedTables") List<ConvertedTable> convertedTables) {
    this.convertedTables = convertedTables;
  }
}
