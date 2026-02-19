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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.AbstractElementVisitor8;
import javax.tools.StandardLocation;
import jdk.javadoc.doclet.DocletEnvironment;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;

/**
 * Processes FeatureConfiguration and BehaviorChangeConfiguration static fields to extract
 * configuration documentation.
 */
public class PolarisConfigurationConfigs {
  private final ClassLoader classLoader;

  private final List<PolarisConfigurationInfo> featureConfigs = new ArrayList<>();
  private final List<PolarisConfigurationInfo> behaviorChangeConfigs = new ArrayList<>();

  public PolarisConfigurationConfigs(DocletEnvironment env) {
    this.classLoader = env.getJavaFileManager().getClassLoader(StandardLocation.CLASS_PATH);
  }

  public List<PolarisConfigurationInfo> featureConfigs() {
    return featureConfigs;
  }

  public List<PolarisConfigurationInfo> behaviorChangeConfigs() {
    return behaviorChangeConfigs;
  }

  ElementVisitor<Void, Void> visitor() {
    return new AbstractElementVisitor8<>() {

      @Override
      public Void visitPackage(PackageElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, Void ignore) {
        var className = e.getQualifiedName().toString();

        if (className.equals("org.apache.polaris.core.config.FeatureConfiguration")
            || className.equals("org.apache.polaris.core.config.BehaviorChangeConfiguration")) {

          try {
            Class<?> clazz = Class.forName(className, true, classLoader);
            processConfigurationClass(clazz);
          } catch (ClassNotFoundException ex) {
            System.err.println("Warning: Could not load class " + className + ": " + ex);
          }
        }
        return null;
      }

      private void processConfigurationClass(Class<?> clazz) {
        for (var field : clazz.getDeclaredFields()) {
          if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
            continue;
          }
          if (!java.lang.reflect.Modifier.isFinal(field.getModifiers())) {
            continue;
          }
          if (!PolarisConfiguration.class.isAssignableFrom(field.getType())) {
            continue;
          }

          try {
            field.setAccessible(true);
            PolarisConfiguration<?> config = (PolarisConfiguration<?>) field.get(null);
            if (config == null) {
              continue;
            }

            String propertyName =
                (config instanceof FeatureConfiguration
                        ? "polaris.features.\""
                        : "polaris.behavior-changes.\"")
                    + config.key()
                    + "\"";

            String typeString = extractTypeParameter(field.getGenericType());

            var info =
                new PolarisConfigurationInfo(
                    propertyName,
                    config.description(),
                    String.valueOf(config.defaultValue()),
                    config.hasCatalogConfig() ? config.catalogConfig() : null,
                    typeString);

            if (config instanceof FeatureConfiguration) {
              featureConfigs.add(info);
            } else if (config instanceof BehaviorChangeConfiguration) {
              behaviorChangeConfigs.add(info);
            }
          } catch (IllegalAccessException ex) {
            System.err.println("Warning: Could not access field " + field.getName() + ": " + ex);
          }
        }
      }

      private String extractTypeParameter(Type genericType) {
        if (genericType instanceof ParameterizedType parameterizedType) {
          Type[] typeArgs = parameterizedType.getActualTypeArguments();
          if (typeArgs.length > 0) {
            return formatType(typeArgs[0]);
          }
        }
        return "unknown";
      }

      private String formatType(Type type) {
        if (type instanceof ParameterizedType parameterizedType) {
          Type rawType = parameterizedType.getRawType();
          Type[] typeArgs = parameterizedType.getActualTypeArguments();
          StringBuilder sb = new StringBuilder();
          sb.append(getSimpleName(rawType));
          sb.append("<");
          for (int i = 0; i < typeArgs.length; i++) {
            if (i > 0) {
              sb.append(", ");
            }
            sb.append(formatType(typeArgs[i]));
          }
          sb.append(">");
          return sb.toString();
        } else if (type instanceof Class<?> clz) {
          return clz.getSimpleName();
        } else {
          return type.getTypeName();
        }
      }

      private String getSimpleName(Type type) {
        if (type instanceof Class<?> clz) {
          return clz.getSimpleName();
        }
        String typeName = type.getTypeName();
        int lastDot = typeName.lastIndexOf('.');
        return lastDot >= 0 ? typeName.substring(lastDot + 1) : typeName;
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
