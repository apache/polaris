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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableStorageObject.class)
@JsonDeserialize(as = ImmutableStorageObject.class)
@Value.Immutable
// See https://cloud.google.com/storage/docs/json_api/v1/objects#resource
public interface StorageObject {
  default String kind() {
    return "storage#object";
  }

  String id();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String selfLink();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String mediaLink();

  String name();

  String bucket();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long generation();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long metageneration();

  String contentType();

  String storageClass();

  @JsonSerialize(using = ToStringSerializer.class)
  long size();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant softDeleteTime();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant hardDeleteTime();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String md5Hash();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String contentEncoding();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String contentDisposition();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String contentLanguage();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String cacheControl();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String crc32c();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer componentCount();

  String etag();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String kmsKeyName();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Boolean temporaryHold();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Boolean eventBasedHold();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String retentionExpirationTime();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  JsonNode retention();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant timeCreated();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant updated();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant timeDeleted();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant timeStorageClassUpdated();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant customTime();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  JsonNode metadata();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  JsonNode acl();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  JsonNode customerEncryption();
}
