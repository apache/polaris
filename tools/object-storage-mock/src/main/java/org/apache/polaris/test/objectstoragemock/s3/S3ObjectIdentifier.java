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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableS3ObjectIdentifier.class)
@JsonDeserialize(as = ImmutableS3ObjectIdentifier.class)
@Value.Immutable
public interface S3ObjectIdentifier extends ObjectIdentifier {
  static S3ObjectIdentifier of(String key) {
    return of(key, null);
  }

  static S3ObjectIdentifier of(String key, String versionId) {
    return ImmutableS3ObjectIdentifier.of(key, versionId);
  }
}
