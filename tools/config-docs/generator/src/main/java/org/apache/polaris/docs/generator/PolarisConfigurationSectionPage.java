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
import java.util.ArrayList;
import java.util.List;

/**
 * Generates a markdown page section for PolarisConfiguration (FeatureConfiguration or
 * BehaviorChangeConfiguration).
 */
public class PolarisConfigurationSectionPage {

  private final String sectionDescription;
  private final List<String> lines = new ArrayList<>();
  private boolean empty = true;

  public PolarisConfigurationSectionPage(String sectionDescription) {
    this.sectionDescription = sectionDescription;
  }

  public void addProperty(PolarisConfigurationInfo info) {
    if (!empty) {
      // Add separator between properties
      lines.add("");
      lines.add("---");
      lines.add("");
    }
    empty = false;

    // Property name as heading
    lines.add("##### `" + info.propertyName() + "`");
    lines.add("");
    // Description
    lines.add(info.description().replaceAll("\\s+", " ").trim());
    lines.add("");
    // Metadata as a list
    lines.add("- **Type:** `" + info.type() + "`");
    lines.add("- **Default:** `" + info.defaultValue() + "`");
    if (info.catalogConfig() != null) {
      lines.add("- **Catalog Config:** `" + info.catalogConfig() + "`");
    }
  }

  public boolean isEmpty() {
    return empty;
  }

  public void writeTo(PrintWriter pw) {
    if (sectionDescription != null && !sectionDescription.isEmpty()) {
      pw.println(sectionDescription);
      pw.println();
    }
    lines.forEach(pw::println);
    pw.println();
    pw.println("---");
  }
}
