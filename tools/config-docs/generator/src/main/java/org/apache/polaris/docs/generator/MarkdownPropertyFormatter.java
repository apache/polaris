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

public class MarkdownPropertyFormatter extends MarkdownFormatter {

  private final PropertyInfo propertyInfo;

  public MarkdownPropertyFormatter(PropertyInfo propertyInfo) {
    super(propertyInfo.propertyElement(), propertyInfo.doc());
    this.propertyInfo = propertyInfo;
  }

  public String propertyName() {
    return propertyInfo.propertyName();
  }

  public String propertySuffix() {
    return propertyInfo.propertySuffix();
  }

  public String propertyType() {
    return '`' + propertyInfo.simplifiedTypeName() + '`';
  }
}
