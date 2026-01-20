/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;
import org.immutables.value.Value;

@JsonRootName("ListPartsResult")
@JsonSerialize(as = ImmutableListPartsResult.class)
@JsonDeserialize(as = ImmutableListPartsResult.class)
@Value.Immutable
@Value.Style(jdkOnly = true)
public interface ListPartsResult {

  @JsonProperty("Bucket")
  String bucket();

  @JsonProperty("Key")
  String key();

  @JsonProperty("UploadId")
  String uploadId();

  @JsonProperty("PartNumberMarker")
  @Value.Default
  default String partNumberMarker() {
    return "0";
  }

  @JsonProperty("NextPartNumberMarker")
  @Value.Default
  default String nextPartNumberMarker() {
    return "1";
  }

  @JsonProperty("IsTruncated")
  @Value.Default
  default boolean truncated() {
    return false;
  }

  @JsonProperty("StorageClass")
  @Value.Default
  default StorageClass storageClass() {
    return StorageClass.STANDARD;
  }

  @JsonProperty("Part")
  @JacksonXmlElementWrapper(useWrapping = false)
  List<Part> parts();
}
