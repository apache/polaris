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

import java.time.Duration;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
public interface PurgeStats {
  /** Runtime of the purge operation. */
  Duration duration();

  /**
   * The number of files attempted to be purged, only for informational purposes.
   *
   * <p>The accuracy of the returned value depends on the information provided by the leveraged
   * libraries, for example {@link BulkDeletionFailureException#numberFailedObjects()}.
   */
  long purgeFileRequests();

  /**
   * Number of files that were not purged, only for informational purposes.
   *
   * <p>The accuracy of the returned value depends on the information provided by the leveraged
   * libraries, for example {@link BulkDeletionFailureException#numberFailedObjects()}.
   */
  long failedFilePurges();
}
