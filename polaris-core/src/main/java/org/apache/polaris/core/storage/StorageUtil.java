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
package org.apache.polaris.core.storage;

import java.net.URI;
import org.jetbrains.annotations.NotNull;

public class StorageUtil {
  /**
   * Concatenating two file paths by making sure one and only one path separator is placed between
   * the two paths.
   *
   * @param leftPath left path
   * @param rightPath right path
   * @param fileSep File separator to use.
   * @return Well formatted file path.
   */
  public static @NotNull String concatFilePrefixes(
      @NotNull String leftPath, String rightPath, String fileSep) {
    if (leftPath.endsWith(fileSep) && rightPath.startsWith(fileSep)) {
      return leftPath + rightPath.substring(1);
    } else if (!leftPath.endsWith(fileSep) && !rightPath.startsWith(fileSep)) {
      return leftPath + fileSep + rightPath;
    } else {
      return leftPath + rightPath;
    }
  }

  /**
   * Given a path, extract the bucket (authority).
   *
   * @param path A path to parse
   * @return The bucket/authority of the path
   */
  public static @NotNull String getBucket(String path) {
    URI uri = URI.create(path);
    return getBucket(uri);
  }

  /**
   * Given a URI, extract the bucket (authority).
   *
   * @param uri A path to parse
   * @return The bucket/authority of the URI
   */
  public static @NotNull String getBucket(URI uri) {
    return uri.getAuthority();
  }
}
