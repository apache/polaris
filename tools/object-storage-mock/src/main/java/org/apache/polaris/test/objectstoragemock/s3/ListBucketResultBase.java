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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import jakarta.annotation.Nullable;
import java.util.List;

public interface ListBucketResultBase {
  interface Builder<B extends Builder<B>> {
    B name(String name);

    B prefix(String prefix);

    B maxKeys(int maxKeys);

    B encodingType(String encodingType);

    B isTruncated(boolean isTruncated);

    B addContents(S3Object prefix);

    B addContents(S3Object... elements);

    B addAllContents(Iterable<? extends S3Object> elements);

    B addCommonPrefixes(Prefix prefix);

    B addCommonPrefixes(Prefix... elements);

    B addAllCommonPrefixes(Iterable<? extends Prefix> elements);
  }

  @JsonProperty("Name")
  String name();

  @JsonProperty("Prefix")
  @Nullable
  String prefix();

  @JsonProperty("MaxKeys")
  int maxKeys();

  @JsonProperty("EncodingType")
  @Nullable
  String encodingType();

  @JsonProperty("IsTruncated")
  boolean isTruncated();

  @JsonProperty("Contents")
  @JacksonXmlElementWrapper(useWrapping = false)
  List<S3Object> contents();

  @JsonProperty("CommonPrefixes")
  @JacksonXmlElementWrapper(useWrapping = false)
  List<Prefix> commonPrefixes();
}
