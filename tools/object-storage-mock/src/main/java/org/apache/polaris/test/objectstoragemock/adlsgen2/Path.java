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
package org.apache.polaris.test.objectstoragemock.adlsgen2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutablePath.class)
@JsonDeserialize(as = ImmutablePath.class)
@Value.Immutable
// See
// https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12#path
public interface Path {
  long contentLength();

  @JsonProperty("eTag")
  String etag();

  @Nullable
  String group();

  boolean isDirectory();

  String lastModified();

  /**
   * Not mentioned in the specs, but required by the ADLS client. See {@code
   * com.azure.storage.file.datalake.models.PathItem}.
   */
  long creationTime();

  String name();

  @Nullable
  String owner();

  @Nullable
  String permissions();

  @JsonProperty("x-ms-encryption-context")
  @Nullable
  String msEncryptionContext();

  @JsonProperty("x-ms-encryption-key-sha256")
  @Nullable
  String msEncryptionKeySha256();

  @JsonProperty("x-ms-encryption-scope")
  @Nullable
  String msEncryptionScope();

  @JsonProperty("x-ms-server-encrypted")
  @Nullable
  Boolean msServerEncrypted();
}
