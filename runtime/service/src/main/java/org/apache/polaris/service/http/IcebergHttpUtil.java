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
package org.apache.polaris.service.http;

import org.apache.polaris.core.DigestUtils;

/** Utility class that encapsulates logic pertaining to Iceberg REST specific concepts. */
public class IcebergHttpUtil {

  private IcebergHttpUtil() {}

  /**
   * Generate an ETag from an Iceberg metadata file location
   *
   * @param metadataFileLocation
   * @return the generated ETag
   */
  public static String generateETagForMetadataFileLocation(String metadataFileLocation) {
    if (metadataFileLocation == null) {
      // Throw a more appropriate exception than letting DigestUtils die randomly.
      throw new IllegalArgumentException("Unable to generate etag for null metadataFileLocation");
    }

    // Use hash of metadata location since we don't want clients to use the ETag to try to extract
    // the metadata file location
    String hashedMetadataFileLocation = DigestUtils.sha256Hex(metadataFileLocation);

    // always issue a weak ETag since we semantically compare metadata, not its content byte-by-byte
    return "W/\"" + hashedMetadataFileLocation + "\"";
  }
}
