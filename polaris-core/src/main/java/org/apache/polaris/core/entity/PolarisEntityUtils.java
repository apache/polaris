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
package org.apache.polaris.core.entity;

import com.google.common.base.Preconditions;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;

public final class PolarisEntityUtils {

  private static final String NAMESPACE_SEPARATOR_ENCODED = "%1F";

  private PolarisEntityUtils() {}

  /**
   * Returns a String representation of a namespace that is suitable for storage in entity internal
   * properties.
   *
   * <p>This method is similar to {@link RESTUtil#encodeNamespace(Namespace)}.
   */
  public static String encodeNamespace(Namespace ns) {
    Preconditions.checkArgument(ns != null, "Invalid namespace: null");
    String[] levels = ns.levels();
    String[] encodedLevels = new String[levels.length];
    for (int i = 0; i < levels.length; i++) {
      encodedLevels[i] = URLEncoder.encode(levels[i], StandardCharsets.UTF_8);
    }
    return String.join(NAMESPACE_SEPARATOR_ENCODED, encodedLevels);
  }

  /**
   * Returns a Namespace object from a String representation that was encoded using {@link
   * #encodeNamespace(Namespace)}.
   *
   * <p>This method is similar to {@link RESTUtil#decodeNamespace(String)}.
   */
  public static Namespace decodeNamespace(String encodedNs) {
    Preconditions.checkArgument(encodedNs != null, "Invalid namespace: null");
    String[] levels = encodedNs.split(NAMESPACE_SEPARATOR_ENCODED, -1);
    for (int i = 0; i < levels.length; i++) {
      levels[i] = URLDecoder.decode(levels[i], StandardCharsets.UTF_8);
    }
    return Namespace.of(levels);
  }
}
