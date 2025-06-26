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
package org.apache.polaris.docs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Contains annotations for {@code :polaris-config-doc-generator}. */
public interface ConfigDocs {
  /**
   * For properties-configs, declares a class containing configuration constants for "properties"
   * and gives it a page name. The generated markdown files will start with this name.
   */
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.SOURCE)
  @interface ConfigPageGroup {
    String name();
  }

  /**
   * Define the "section" in which the config option appears.
   *
   * <p>For properties-configs, this declares that a property constant field appears in the
   * generated markdown files.
   *
   * <p>For smallrye-configs, this declares that a property appears under a different "prefix".
   */
  @Target({ElementType.FIELD, ElementType.METHOD})
  @Retention(RetentionPolicy.SOURCE)
  @interface ConfigItem {
    /**
     * The name of the "section" in which this constant field shall appear. The name of the
     * generated markdown file will "end" with this name.
     */
    String section() default "";

    /** For smallrye-configs only: the section docs are taken from property type's javadoc. */
    boolean sectionDocFromType() default false;
  }

  /**
   * For smallrye-configs, gives map-keys a name that appears as a placeholder in the fully
   * qualified config name in the generated docs.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.SOURCE)
  @interface ConfigPropertyName {
    String value() default "";
  }
}
