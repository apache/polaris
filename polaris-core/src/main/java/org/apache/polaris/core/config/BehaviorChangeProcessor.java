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
package org.apache.polaris.core.config;

import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

@SupportedAnnotationTypes("BehaviorChange")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class BehaviorChangeProcessor extends AbstractProcessor {

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    Elements elementUtils = processingEnv.getElementUtils();

    for (Element element : roundEnv.getRootElements()) {
      if (element.getKind() == ElementKind.CLASS) {
        TypeElement typeElement = (TypeElement) element;
        for (Element enclosedElement : typeElement.getEnclosedElements()) {
          if (enclosedElement.getKind() == ElementKind.FIELD) {
            VariableElement field = (VariableElement) enclosedElement;
            TypeMirror fieldType = field.asType();

            if (isBehaviorChangeConfiguration(fieldType, elementUtils)) {
              if (field.getAnnotation(BehaviorChange.class) == null) {
                processingEnv
                    .getMessager()
                    .printMessage(
                        Diagnostic.Kind.ERROR,
                        "Field "
                            + field.getSimpleName()
                            + " of type BehaviorChangeConfiguration<?> must be annotated with @BehaviorChange",
                        field);
              } else {
                processingEnv
                    .getMessager()
                    .printMessage(
                        Diagnostic.Kind.NOTE,
                        "Field "
                            + field.getSimpleName()
                            + " is correctly annotated with @BehaviorChange",
                        field);
              }
            }
          }
        }
      }
    }
    return true;
  }

  private boolean isBehaviorChangeConfiguration(TypeMirror typeMirror, Elements elementUtils) {
    TypeElement typeElement =
        (TypeElement) elementUtils.getTypeElement("BehaviorChangeConfiguration");
    if (typeElement == null) {
      return false;
    }
    TypeMirror behaviorChangeConfigurationType = typeElement.asType();
    return processingEnv.getTypeUtils().isAssignable(typeMirror, behaviorChangeConfigurationType);
  }
}
