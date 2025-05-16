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

import com.sun.source.doctree.DocCommentTree;
import java.util.Optional;
import javax.lang.model.element.Element;
import javax.lang.model.element.VariableElement;

public class PropertiesConfigItem implements PropertyInfo {
  private final VariableElement field;
  private final DocCommentTree doc;

  public PropertiesConfigItem(VariableElement field, DocCommentTree doc) {
    this.field = field;
    this.doc = doc;
  }

  @Override
  public Element propertyElement() {
    return field;
  }

  @Override
  public DocCommentTree doc() {
    return doc;
  }

  @Override
  public String defaultValue() {
    return "";
  }

  @Override
  public String simplifiedTypeName() {
    return "";
  }

  @Override
  public String propertyName() {
    return field.getConstantValue().toString();
  }

  @Override
  public String propertySuffix() {
    return "";
  }

  @Override
  public Optional<Class<?>> groupType() {
    return Optional.empty();
  }
}
