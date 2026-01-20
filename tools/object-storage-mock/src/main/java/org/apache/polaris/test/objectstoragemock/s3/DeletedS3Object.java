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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonSerialize(as = ImmutableDeletedS3Object.class)
@JsonDeserialize(as = ImmutableDeletedS3Object.class)
@Value.Immutable
public interface DeletedS3Object extends ObjectIdentifier {

  @JsonProperty("DeleteMarker")
  @Nullable
  Boolean deleteMarker();

  @JsonProperty("DeleteMarkerVersionId")
  @Nullable
  String deleteMarkerVersionId();

  static DeletedS3Object from(S3ObjectIdentifier s3ObjectIdentifier) {
    return ImmutableDeletedS3Object.of(s3ObjectIdentifier.key(), s3ObjectIdentifier.versionId());
  }
}
