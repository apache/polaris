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

import com.sun.source.doctree.DocCommentTree;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.TypeElement;

public class SmallRyeConfigSectionPage {
  final String section;
  final List<String> lines = new ArrayList<>();
  final TypeElement typeElement;
  DocCommentTree comment;
  boolean empty = true;
  int sectionRef;

  SmallRyeConfigSectionPage(String section, TypeElement typeElement, DocCommentTree comment) {
    this.section = section;
    this.typeElement = typeElement;
    this.comment = comment;
  }

  public void addProperty(
      String propertyFullName,
      SmallRyeConfigPropertyInfo propertyInfo,
      MarkdownPropertyFormatter md) {
    if (empty) {
      lines.add("| Property | Default Value | Type | Description |");
      lines.add("|----------|---------------|------|-------------|");

      empty = false;
    }

    var fullNameCode = ('`' + propertyFullName + '`').replaceAll("``", "");
    var dv = propertyInfo.defaultValue();

    lines.add(
        "| "
            + fullNameCode
            + " | "
            + (dv != null ? (dv.isEmpty() ? "(empty)" : '`' + dv + '`') : "")
            + " | "
            + md.propertyType()
            + " | "
            + md.description().replaceAll("\n", "<br>")
            + " |");
  }

  public boolean isEmpty() {
    return comment == null && empty;
  }

  public void writeTo(PrintWriter pw) {
    if (comment != null) {
      var typeFormatter = new MarkdownTypeFormatter(typeElement, comment);
      pw.println(typeFormatter.description().trim());
      pw.println();
    }

    lines.forEach(pw::println);
  }

  public void incrementSectionRef() {
    if (++sectionRef > 1) {
      comment = null;
    }
  }
}
