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
package org.apache.polaris.service.catalog.tag;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ext.ParamConverter;
import jakarta.ws.rs.ext.ParamConverterProvider;
import jakarta.ws.rs.ext.Provider;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import org.apache.polaris.service.types.TargetType;

/**
 * Binds an unrecognized {@code target-type} query value to 400, not the 404 a plain JAX-RS enum
 * conversion failure would otherwise answer.
 *
 * <p>{@code @QueryParam("target-type") @NotNull TargetType} has no hand-written converter to fall
 * back on: a bad value fails inside the framework's own enum binding, which is a {@code
 * NotFoundException} the generic exception mapper turns into 404. The contract requires an
 * unsupported target kind to be 400 instead, so this provider intercepts the conversion for {@code
 * TargetType} specifically, before the framework's default binding ever runs.
 *
 * <p>The exception type is the load-bearing part. The framework wraps anything that is not a {@link
 * jakarta.ws.rs.WebApplicationException} thrown while binding a query parameter in a {@code
 * NotFoundException} before any exception mapper runs, so a plain runtime exception here would
 * still answer 404. A {@code WebApplicationException} propagates instead, and carrying no entity it
 * reaches the mappers, which render its 400 and the {@code BadRequest} error type the contract
 * names.
 *
 * <p>Scoped to {@code TargetType} alone: {@link #getConverter} returns {@code null} for every other
 * type, which asks the framework to keep using its own default converter there, so no other
 * endpoint's enum binding changes.
 */
@Provider
public class TargetTypeParamConverterProvider implements ParamConverterProvider {

  @Override
  @SuppressWarnings("unchecked")
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (rawType != TargetType.class) {
      return null;
    }
    return (ParamConverter<T>) new TargetTypeParamConverter();
  }

  private static final class TargetTypeParamConverter implements ParamConverter<TargetType> {
    @Override
    public TargetType fromString(String value) {
      if (value == null) {
        return null;
      }
      try {
        return TargetType.valueOf(value);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unsupported target type: " + value);
      }
    }

    @Override
    public String toString(TargetType value) {
      return value == null ? null : value.toString();
    }
  }
}
