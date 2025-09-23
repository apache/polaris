/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.storage.files.api;

import static com.google.common.base.Preconditions.checkState;

import java.util.OptionalDouble;
import java.util.function.Consumer;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@SuppressWarnings("unused")
@PolarisImmutable
public interface PurgeSpec {
  PurgeSpec DEFAULT_INSTANCE = PurgeSpec.builder().build();

  @Value.Default
  default FileFilter fileFilter() {
    return FileFilter.alwaysTrue();
  }

  PurgeSpec withFileFilter(FileFilter fileFilter);

  /**
   * Delete batch size for purge/batch-deletion operations. Implementations may opt to ignore this
   * parameter and enforce a reasonable or required different limit.
   */
  @Value.Default
  default int deleteBatchSize() {
    return 250;
  }

  PurgeSpec withDeleteBatchSize(int deleteBatchSize);

  /**
   * Callback being invoked right before a file location is being submitted to be purged.
   *
   * <p>Due to API constraints of {@link
   * org.apache.iceberg.io.SupportsBulkOperations#deleteFiles(Iterable)} it's barely possible to
   * identify files that failed a deletion.
   */
  @Value.Default
  default Consumer<String> purgeIssuedCallback() {
    return location -> {};
  }

  PurgeSpec withPurgeIssuedCallback(Consumer<String> purgeIssuedCallback);

  /**
   * Optional rate-limit on the number of individual file-deletions per second.
   *
   * <p>This setting is usually similar to using {@link #batchDeletesPerSecond()} times {@link
   * #deleteBatchSize()}, unless the implementaiton opted to choose a different batch size.
   *
   * @see #batchDeletesPerSecond()
   */
  OptionalDouble fileDeletesPerSecond();

  PurgeSpec withFileDeletesPerSecond(OptionalDouble fileDeletesPerSecond);

  PurgeSpec withFileDeletesPerSecond(double fileDeletesPerSecond);

  /**
   * Optional rate-limit on batch-delete operations per second
   *
   * @see #fileDeletesPerSecond()
   */
  OptionalDouble batchDeletesPerSecond();

  PurgeSpec withBatchDeletesPerSecond(OptionalDouble batchDeletesPerSecond);

  PurgeSpec withBatchDeletesPerSecond(double batchDeletesPerSecond);

  static ImmutablePurgeSpec.Builder builder() {
    return ImmutablePurgeSpec.builder();
  }

  @Value.Check
  default void check() {
    checkState(deleteBatchSize() > 0, "deleteBatchSize must be positive");
  }
}
