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

package org.apache.polaris.service.catalog.common;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

/**
 * A collection of utilities related to table locations CODE_COPIED_TO_POLARIS From Apache Iceberg
 * Version: 1.9.1
 */
public class LocationUtils {

  private static final HashFunction HASH_FUNC = Hashing.murmur3_32_fixed();
  // Length of entropy generated in the file location
  public static final int HASH_BINARY_STRING_BITS = 20;
  // Entropy generated will be divided into dirs with this lengths
  public static final int ENTROPY_DIR_LENGTH = 4;
  // Will create DEPTH many dirs from the entropy
  public static final int ENTROPY_DIR_DEPTH = 3;

  /**
   * Given a file path, compute a path fragment derived from its hash. This is taken from
   * LocationProviders.computeHash in Iceberg.
   *
   * @param fileName file.txt
   * @return 1001/1001/1001/10011001
   */
  public static String computeHash(@Nonnull String fileName) {
    HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);

    // {@link Integer#toBinaryString} excludes leading zeros, which we want to preserve.
    // force the first bit to be set to get around that.
    String hashAsBinaryString = Integer.toBinaryString(hashCode.asInt() | Integer.MIN_VALUE);
    // Limit hash length to HASH_BINARY_STRING_BITS
    String hash =
        hashAsBinaryString.substring(hashAsBinaryString.length() - HASH_BINARY_STRING_BITS);
    return dirsFromHash(hash);
  }

  /**
   * Divides hash into directories for optimized orphan removal operation using ENTROPY_DIR_DEPTH
   * and ENTROPY_DIR_LENGTH
   *
   * @param hash 10011001100110011001
   * @return 1001/1001/1001/10011001 with depth 3 and length 4
   */
  private static String dirsFromHash(String hash) {
    StringBuilder hashWithDirs = new StringBuilder();

    for (int i = 0; i < ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH; i += ENTROPY_DIR_LENGTH) {
      if (i > 0) {
        hashWithDirs.append("/");
      }
      hashWithDirs.append(hash, i, Math.min(i + ENTROPY_DIR_LENGTH, hash.length()));
    }

    if (hash.length() > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH) {
      hashWithDirs.append("/").append(hash, ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH, hash.length());
    }

    return hashWithDirs.toString();
  }
}
