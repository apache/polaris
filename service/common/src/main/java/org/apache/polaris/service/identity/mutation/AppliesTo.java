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

package org.apache.polaris.service.identity.mutation;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.*;
import org.apache.polaris.core.identity.mutation.EntityMutationEngine;
import org.apache.polaris.core.identity.mutation.EntityMutator;
import org.apache.polaris.core.identity.mutation.MutationPoint;

/**
 * Qualifier to mark an {@link EntityMutator} as applicable to a specific {@link MutationPoint}.
 *
 * <p>This is used by the {@link EntityMutationEngine} to apply only relevant mutators based on
 * context.
 *
 * <p>Supports being repeated on the same class to handle multiple mutation points.
 */
@Qualifier
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(AppliesTos.class)
public @interface AppliesTo {

  /** The mutation point this mutator applies to. */
  MutationPoint value();

  /** Helper for creating {@link AppliesTo} qualifiers programmatically. */
  final class Literal extends AnnotationLiteral<AppliesTo> implements AppliesTo {
    private final MutationPoint value;

    public static Literal of(MutationPoint value) {
      return new Literal(value);
    }

    private Literal(MutationPoint value) {
      this.value = value;
    }

    @Override
    public MutationPoint value() {
      return value;
    }
  }
}
