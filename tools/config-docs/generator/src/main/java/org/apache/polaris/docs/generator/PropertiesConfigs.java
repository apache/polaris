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

import java.util.HashMap;
import java.util.Map;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.AbstractElementVisitor8;
import jdk.javadoc.doclet.DocletEnvironment;
import org.apache.polaris.docs.ConfigDocs.ConfigPageGroup;

public class PropertiesConfigs {
  private final DocletEnvironment env;
  private final Map<String, PropertiesConfigPageGroup> pages = new HashMap<>();

  public PropertiesConfigs(DocletEnvironment env) {
    this.env = env;
  }

  public PropertiesConfigPageGroup page(String name) {
    return pages.computeIfAbsent(name, PropertiesConfigPageGroup::new);
  }

  public Iterable<PropertiesConfigPageGroup> pages() {
    return pages.values();
  }

  public ElementVisitor<Void, Void> visitor() {
    return new AbstractElementVisitor8<>() {
      @Override
      public Void visitPackage(PackageElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, Void ignore) {
        ConfigPageGroup configPageGroup = e.getAnnotation(ConfigPageGroup.class);
        if (configPageGroup != null) {
          PropertiesConfigPageGroup page = page(configPageGroup.name());

          e.getEnclosedElements().forEach(element -> element.accept(page.visitor(), env));
        }
        return null;
      }

      @Override
      public Void visitVariable(VariableElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitExecutable(ExecutableElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitTypeParameter(TypeParameterElement e, Void ignore) {
        return null;
      }
    };
  }
}
