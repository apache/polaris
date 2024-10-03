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
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;

public class TaskUtils {
  static boolean exists(String path, FileIO fileIO) {
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
  static String encodeManifestFile(ManifestFile mf) {
    try {
      return Base64.encodeBase64String(ManifestFiles.encode(mf));
    } catch (IOException e) {
      throw new RuntimeException("Unable to encode binary data in memory", e);
    }
  }
}
