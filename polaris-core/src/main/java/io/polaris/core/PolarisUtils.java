/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A collection of utilities for polaris-core */
public class PolarisUtils {

  /** Given some path, append a `/` if it doesn't already end with one */
  public static String terminateWithSlash(String path) {
    if (path == null) {
      return null;
    } else if (!path.endsWith("/")) {
      return path + "/";
    } else {
      return path;
    }
  }

  /**
   * Given a path like `/a/b/c`, find all directories in the path such as [`/`, `/a/`, `/a/b/`,
   * `/a/b/c`]
   */
  public static Optional<List<String>> pathToDirectories(String path, int maxSegments) {
    List<String> directories = new ArrayList<>();
    String[] splitPath = path.split("/", -1);

    if (splitPath.length - 2 > maxSegments) {
      return Optional.empty();
    } else {
      StringBuilder currentPath = new StringBuilder();
      for (int i = 0; i < splitPath.length - 1; i++) {
        currentPath.append(splitPath[i]).append("/");
        directories.add(currentPath.toString());
      }
      // Chop off the protocol:
      return Optional.of(directories.subList(1, directories.size()));
    }
  }
}
