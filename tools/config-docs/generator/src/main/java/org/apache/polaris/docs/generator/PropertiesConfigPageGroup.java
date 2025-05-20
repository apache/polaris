/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.docs.generator;

import static javax.lang.model.element.ElementKind.FIELD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.AbstractElementVisitor8;
import jdk.javadoc.doclet.DocletEnvironment;
import org.apache.polaris.docs.ConfigDocs.ConfigItem;

public class PropertiesConfigPageGroup {

  public static final Set<Modifier> EXPECTED_FIELD_MODIFIERS =
      Set.of(Modifier.FINAL, Modifier.STATIC);

  private final String name;

  private final Map<String, List<PropertiesConfigItem>> items = new LinkedHashMap<>();

  public PropertiesConfigPageGroup(String name) {
    this.name = name;
  }

  public Map<String, Iterable<PropertiesConfigItem>> sectionItems() {
    var sections = new LinkedHashMap<String, Iterable<PropertiesConfigItem>>();
    forEachSection(sections::put);
    return sections;
  }

  public void forEachSection(BiConsumer<String, Iterable<PropertiesConfigItem>> consumer) {
    var processedSections = new HashSet<String>();

    for (var e : items.entrySet()) {
      var section = e.getKey();
      if (processedSections.add(section)) {
        consumer.accept(section, e.getValue());
      }
    }
  }

  public String name() {
    return name;
  }

  ElementVisitor<Void, DocletEnvironment> visitor() {
    return new AbstractElementVisitor8<>() {
      @Override
      public Void visitPackage(PackageElement e, DocletEnvironment env) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, DocletEnvironment env) {
        return e.accept(this, env);
      }

      @Override
      public Void visitVariable(VariableElement e, DocletEnvironment env) {
        var configItem = e.getAnnotation(ConfigItem.class);
        if (configItem != null) {
          var propertyKey = e.getConstantValue();
          if (e.getKind() == FIELD
              && e.getModifiers().containsAll(EXPECTED_FIELD_MODIFIERS)
              && propertyKey instanceof String) {
            var docComment = env.getDocTrees().getDocCommentTree(e);

            items
                .computeIfAbsent(configItem.section(), g -> new ArrayList<>())
                .add(new PropertiesConfigItem(e, docComment));
          }
          // else: @ConfigItem placed on something else than a constant String field.
        }
        return null;
      }

      @Override
      public Void visitExecutable(ExecutableElement e, DocletEnvironment env) {
        return null;
      }

      @Override
      public Void visitTypeParameter(TypeParameterElement e, DocletEnvironment env) {
        return null;
      }
    };
  }
}
