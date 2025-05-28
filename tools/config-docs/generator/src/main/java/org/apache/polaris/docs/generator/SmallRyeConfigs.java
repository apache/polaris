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

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.ConfigMappingInterface;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

public class SmallRyeConfigs {
  private final DocletEnvironment env;

  private final ClassLoader classLoader;

  private final Map<String, SmallRyeConfigMappingInfo> configMappingInfos = new HashMap<>();
  private final Map<String, SmallRyeConfigMappingInfo> configMappingByType = new HashMap<>();

  public SmallRyeConfigs(DocletEnvironment env) {
    this.classLoader = env.getJavaFileManager().getClassLoader(StandardLocation.CLASS_PATH);
    this.env = env;
  }

  Collection<SmallRyeConfigMappingInfo> configMappingInfos() {
    return configMappingInfos.values();
  }

  static String concatWithDot(String s1, String s2) {
    var sb = new StringBuilder();
    var v1 = s1 != null && !s1.isEmpty();
    var v2 = s2 != null && !s2.isEmpty();
    if (v1) {
      sb.append(s1);
    }
    if (v1 && v2) {
      sb.append('.');
    }
    if (v2) {
      sb.append(s2);
    }
    return sb.toString();
  }

  TypeElement getTypeElement(String typeName) {
    var elem = env.getElementUtils().getTypeElement(typeName);
    if (elem == null) {
      elem = env.getElementUtils().getTypeElement(typeName.replace('$', '.'));
    }
    return requireNonNull(elem, "Could not find type '" + typeName + "'");
  }

  public SmallRyeConfigMappingInfo getConfigMappingInfo(Class<?> type) {
    var typeName = type.getName();
    var info = configMappingByType.get(typeName);
    if (info == null) {
      info = new SmallRyeConfigMappingInfo("");
      configMappingByType.put(typeName, info);
      getTypeElement(typeName).accept(visitor(), null);
    }
    return info;
  }

  ElementVisitor<Void, Void> visitor() {
    return new AbstractElementVisitor8<>() {

      @Override
      public Void visitPackage(PackageElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, Void ignore) {
        switch (e.getKind()) {
          case CLASS:
          case INTERFACE:
            var configMapping = e.getAnnotation(ConfigMapping.class);
            SmallRyeConfigMappingInfo mappingInfo;
            ConfigMappingInterface configMappingInterface;

            var className = e.getQualifiedName().toString();

            Class<?> clazz;
            if (configMapping != null) {

              mappingInfo =
                  configMappingInfos.computeIfAbsent(
                      configMapping.prefix(), SmallRyeConfigMappingInfo::new);

              clazz = loadClass(className);
              try {
                configMappingInterface = ConfigMappingInterface.getConfigurationInterface(clazz);
              } catch (RuntimeException ex) {
                throw new RuntimeException("Failed to process mapped " + clazz, ex);
              }
              configMappingByType.put(
                  configMappingInterface.getInterfaceType().getName(), mappingInfo);
            } else {
              mappingInfo = configMappingByType.get(className);
              if (mappingInfo == null) {
                return null;
              }
              clazz = loadClass(className);
              try {
                configMappingInterface = ConfigMappingInterface.getConfigurationInterface(clazz);
              } catch (RuntimeException ex) {
                throw new RuntimeException("Failed to process implicitly added " + clazz, ex);
              }
            }

            mappingInfo.processType(env, configMappingInterface, e);

            var seen = new HashSet<Class<?>>();
            var remaining = new ArrayDeque<>(asList(configMappingInterface.getSuperTypes()));
            while (!remaining.isEmpty()) {
              var superType = remaining.removeFirst();
              if (!seen.add(superType.getInterfaceType())) {
                continue;
              }

              remaining.addAll(asList(superType.getSuperTypes()));

              var superTypeElement = getTypeElement(superType.getInterfaceType().getName());
              mappingInfo.processType(env, superType, superTypeElement);
            }

            // Under some (not really understood) circumstances,
            // ConfigMappingInterface.getSuperTypes() does not return really all extended
            // interfaces. So we traverse the super types via reflection here, skipping all the
            // types that have been visited above.
            var remainingClasses = new ArrayDeque<Class<?>>();
            if (clazz.getSuperclass() != null) {
              remainingClasses.add(clazz.getSuperclass());
            }
            remainingClasses.addAll(asList(clazz.getInterfaces()));
            while (!remainingClasses.isEmpty()) {
              var c = remainingClasses.removeFirst();
              if (!seen.add(c)) {
                continue;
              }

              var superTypeElement = getTypeElement(c.getName());
              mappingInfo.processType(env, configMappingInterface, superTypeElement);

              if (c.getSuperclass() != null) {
                remainingClasses.add(c.getSuperclass());
              }
              remainingClasses.addAll(asList(c.getInterfaces()));
            }
            break;
          default:
            break;
        }

        return null;
      }

      private Class<?> loadClass(String className) {
        try {
          return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex);
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
