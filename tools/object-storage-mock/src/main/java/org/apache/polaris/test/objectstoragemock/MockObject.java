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
package org.apache.polaris.test.objectstoragemock;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.polaris.test.objectstoragemock.s3.StorageClass;
import org.immutables.value.Value;

@Value.Immutable
public interface MockObject {

  static ImmutableMockObject.Builder builder() {
    return ImmutableMockObject.builder();
  }

  @Value.Default
  default String etag() {
    return "etag";
  }

  @Value.Default
  default String contentType() {
    return "application/octet-stream";
  }

  @Value.Default
  default long contentLength() {
    return 0L;
  }

  @Value.Default
  default long lastModified() {
    return 0L;
  }

  @Value.Default
  default StorageClass storageClass() {
    return StorageClass.STANDARD;
  }

  @Value.Default
  default Writer writer() {
    return (range, o) -> {};
  }

  @FunctionalInterface
  interface Writer {
    void write(Range range, OutputStream output) throws IOException;
  }
}
