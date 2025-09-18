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
package org.apache.polaris.service.task;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;

public class TaskUtils {

  public static boolean exists(String path, FileIO fileIO) {
    try {
      return fileIO.newInputFile(path).exists();
    } catch (NotFoundException e) {
      // in-memory FileIO throws this exception
      return false;
    } catch (Exception e) {
      // typically, clients will catch a 404 and simply return false, so any other exception
      // means something probably went wrong
      throw new RuntimeException(e);
    }
  }

  /**
   * base64 encode the serialized manifest file entry so we can deserialize it and read the manifest
   * in the {@link ManifestFileCleanupTaskHandler}
   */
  public static String encodeManifestFile(ManifestFile mf) {
    try {
      byte[] encodedBytes = ManifestFiles.encode(mf);
      return new String(Base64.getEncoder().encode(encodedBytes), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Unable to encode binary data in memory", e);
    }
  }

  public static ManifestFile decodeManifestFileData(String manifestFileData) {
    try {
      byte[] decodedBytes =
          Base64.getDecoder().decode(manifestFileData.getBytes(StandardCharsets.UTF_8));
      return ManifestFiles.decode(decodedBytes);
    } catch (IOException e) {
      throw new RuntimeException("Unable to decode base64 encoded manifest", e);
    }
  }
}
