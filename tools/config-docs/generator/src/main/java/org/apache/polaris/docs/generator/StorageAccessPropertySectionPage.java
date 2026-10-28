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
package org.apache.polaris.docs.generator;

import java.io.PrintWriter;
import java.util.List;

/** Generates a markdown table for a group of {@link StorageAccessPropertyInfo} entries. */
public class StorageAccessPropertySectionPage {

  private final List<StorageAccessPropertyInfo> properties;

  public StorageAccessPropertySectionPage(List<StorageAccessPropertyInfo> properties) {
    this.properties = properties;
  }

  public void writeTo(PrintWriter pw) {
    pw.println("| Property | Type | Kind | Description |");
    pw.println("|----------|------|------|-------------|");
    for (var info : properties) {
      String kind = info.isCredential() ? "credential" : "config";
      String displayName =
          info.isPrefixProperty()
              ? info.propertyName() + ".<" + info.suffixLabel() + ">"
              : info.propertyName();
      // Capitalize first letter of description for consistency
      String desc = info.description();
      if (!desc.isEmpty()) {
        desc = Character.toUpperCase(desc.charAt(0)) + desc.substring(1);
      }
      pw.printf("| `%s` | `%s` | %s | %s |%n", displayName, info.valueType(), kind, desc);
    }
    pw.println();
  }
}
