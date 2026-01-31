/*
 * Copyright (C) 2024 Dremio
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
package org.apache.polaris.test.objectstoragemock.gcs;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableListResponse.class)
@JsonDeserialize(as = ImmutableListResponse.class)
@Value.Immutable
public interface ListResponse {
  default String kind() {
    return "storage#objects";
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String nextPageToken();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> prefixes();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<StorageObject> items();
}
