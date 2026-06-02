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

import static com.google.common.base.Preconditions.checkArgument;

import org.jspecify.annotations.NonNull;

/**
 * Minimal URI parser for storage locations that preserves the full raw path once the path has
 * started, rather than treating {@code ?} or {@code #} inside object keys as query or fragment
 * delimiters.
 *
 * <p>This is an internal utility class and not meant for use outside of this package.
 */
public record StorageUri(String scheme, String authority, String rawPath) {

  @SuppressWarnings("ConstantValue")
  public static @NonNull StorageUri parse(@NonNull String location) {
    checkArgument(location != null, "Invalid storage location uri %s", location);
    int schemeSeparator = location.indexOf("://");
    checkArgument(schemeSeparator > 0, "Invalid storage location uri %s", location);

    int authorityStart = schemeSeparator + 3;
    int pathStart = location.indexOf('/', authorityStart);
    int authorityEnd = pathStart >= 0 ? pathStart : location.length();

    String authority = location.substring(authorityStart, authorityEnd);
    checkArgument(
        authority
            .codePoints()
            .noneMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c)),
        "Invalid storage location uri %s",
        location);

    String rawPath = pathStart >= 0 ? location.substring(pathStart) : "";
    return new StorageUri(
        location.substring(0, schemeSeparator), authority.isEmpty() ? null : authority, rawPath);
  }
}
