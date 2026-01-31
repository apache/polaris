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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

@JsonRootName("ListBucketResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonSerialize(as = ImmutableListBucketResultV2.class)
@JsonDeserialize(as = ImmutableListBucketResultV2.class)
@Value.Immutable
@Value.Style(jdkOnly = true)
public interface ListBucketResultV2 extends ListBucketResultBase {

  static Builder builder() {
    return ImmutableListBucketResultV2.builder();
  }

  interface Builder extends ListBucketResultBase.Builder<Builder> {
    Builder startAfter(String startAfter);

    Builder continuationToken(String continuationToken);

    Builder nextContinuationToken(String nextContinuationToken);

    Builder keyCount(int keyCount);

    ListBucketResultV2 build();
  }

  @JsonProperty("ContinuationToken")
  @Nullable
  String continuationToken();

  @JsonProperty("KeyCount")
  int keyCount();

  @JsonProperty("NextContinuationToken")
  @Nullable
  String nextContinuationToken();

  @JsonProperty("StartAfter")
  @Nullable
  String startAfter();
}
