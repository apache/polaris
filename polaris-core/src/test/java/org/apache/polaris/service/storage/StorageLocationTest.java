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
package org.apache.polaris.service.storage;

import org.apache.polaris.core.storage.StorageLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageLocationTest {

  @Test
  public void testIsChildOfMatchingPrefixes() {
    StorageLocation parentLocation = StorageLocation.of("file:///path/to/file");
    StorageLocation childLocation = StorageLocation.of("file:///path/to/file/child");
    Assertions.assertThat(childLocation.isChildOf(parentLocation)).isTrue();
  }

  @Test
  public void testIsChildOfDifferentPrefixes() {
    StorageLocation parentLocation = StorageLocation.of("file:///path/to/file");
    StorageLocation childLocationLeadingSlash = StorageLocation.of("/path/to/file/child");
    StorageLocation childLocationSingleSlashFile = StorageLocation.of("file:/path/to/file/child");
    Assertions.assertThat(childLocationLeadingSlash.isChildOf(parentLocation)).isTrue();
    Assertions.assertThat(childLocationSingleSlashFile.isChildOf(parentLocation)).isTrue();
  }
}
