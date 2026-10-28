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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.AbstractElementVisitor8;
import javax.tools.StandardLocation;
import jdk.javadoc.doclet.DocletEnvironment;

/**
 * Processes the {@code StorageAccessProperty} enum to extract vended-credentials documentation,
 * grouped by storage type.
 */
public class StorageAccessPropertyConfigs {
  private static final String CLASS_NAME = "org.apache.polaris.core.storage.StorageAccessProperty";

  private final ClassLoader classLoader;

  /** Maps storageType.name() → ordered list of property infos for that storage type. */
  private final Map<String, List<StorageAccessPropertyInfo>> byStorageType = new LinkedHashMap<>();

  public StorageAccessPropertyConfigs(DocletEnvironment env) {
    this.classLoader = env.getJavaFileManager().getClassLoader(StandardLocation.CLASS_PATH);
  }

  /** Returns properties grouped by storage type, in declaration order per group. */
  public Map<String, List<StorageAccessPropertyInfo>> byStorageType() {
    return byStorageType;
  }

  ElementVisitor<Void, Void> visitor() {
    return new AbstractElementVisitor8<>() {

      @Override
      public Void visitPackage(PackageElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, Void ignore) {
        if (!e.getQualifiedName().toString().equals(CLASS_NAME)) {
          return null;
        }
        try {
          Class<?> clazz = Class.forName(CLASS_NAME, true, classLoader);
          processEnum(clazz);
        } catch (ClassNotFoundException ex) {
          System.err.println("Warning: Could not load class " + CLASS_NAME + ": " + ex);
        }
        return null;
      }

      @SuppressWarnings("unchecked")
      private void processEnum(Class<?> clazz) throws ClassNotFoundException {
        if (!clazz.isEnum()) {
          return;
        }
        Method getPropertyName;
        Method getDescription;
        Method getValueType;
        Method isCredential;
        Method getStorageType;
        Method getSuffixLabel;
        try {
          getPropertyName = clazz.getMethod("getPropertyName");
          getDescription = clazz.getMethod("getDescription");
          getValueType = clazz.getMethod("getValueType");
          isCredential = clazz.getMethod("isCredential");
          getStorageType = clazz.getMethod("getStorageType");
          getSuffixLabel = clazz.getMethod("getSuffixLabel");
        } catch (NoSuchMethodException ex) {
          System.err.println("Warning: Missing expected method on " + CLASS_NAME + ": " + ex);
          return;
        }

        for (Object constant : clazz.getEnumConstants()) {
          try {
            String propertyName = (String) getPropertyName.invoke(constant);
            if (propertyName == null || propertyName.isEmpty()) {
              continue;
            }
            String description = (String) getDescription.invoke(constant);
            Class<?> valueType = (Class<?>) getValueType.invoke(constant);
            String typeName = valueType.getSimpleName();
            boolean credential = (boolean) isCredential.invoke(constant);
            Object storageTypeEnum = getStorageType.invoke(constant);
            String storageType = storageTypeEnum.toString();
            String suffixLabel = (String) getSuffixLabel.invoke(constant);

            var info =
                new StorageAccessPropertyInfo(
                    propertyName, description, typeName, credential, storageType, suffixLabel);
            byStorageType.computeIfAbsent(storageType, k -> new ArrayList<>()).add(info);
          } catch (Exception ex) {
            System.err.println("Warning: Error processing constant " + constant + ": " + ex);
          }
        }
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
