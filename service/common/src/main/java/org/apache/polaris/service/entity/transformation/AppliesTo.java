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

package org.apache.polaris.service.entity.transformation;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.polaris.core.entity.transformation.EntityTransformationEngine;
import org.apache.polaris.core.entity.transformation.EntityTransformer;
import org.apache.polaris.core.entity.transformation.TransformationPoint;

/**
 * Qualifier to mark an {@link EntityTransformer} as applicable to a specific {@link
 * TransformationPoint}.
 *
 * <p>This is used by the {@link EntityTransformationEngine} to apply only relevant transformers
 * based on context.
 *
 * <p>Supports being repeated on the same class to handle multiple transformation points.
 */
@Qualifier
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(AppliesTos.class)
public @interface AppliesTo {

  /** The transformation point this transformer applies to. */
  TransformationPoint value();

  /** Helper for creating {@link AppliesTo} qualifiers programmatically. */
  final class Literal extends AnnotationLiteral<AppliesTo> implements AppliesTo {
    private final TransformationPoint value;

    public static Literal of(TransformationPoint value) {
      return new Literal(value);
    }

    private Literal(TransformationPoint value) {
      this.value = value;
    }

    @Override
    public TransformationPoint value() {
      return value;
    }
  }
}
