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
package org.apache.polaris.service.http;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ext.ParamConverter;
import jakarta.ws.rs.ext.ParamConverterProvider;
import jakarta.ws.rs.ext.Provider;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.function.Function;

/**
 * Converts {@code Integer} and {@code Long} request parameters so that a value which is not a
 * number is answered with {@code 400 Bad Request}.
 *
 * <p>Jakarta RESTful Web Services 4.0, section 3.2, requires that an exception thrown while
 * converting a parameter value be wrapped in a {@code NotFoundException} (404) when the parameter
 * is a {@code @QueryParam}, {@code @PathParam} or {@code @MatrixParam}, and in a {@code
 * BadRequestException} (400) when it is a {@code @HeaderParam} or {@code @CookieParam}. The
 * runtime's own numeric converter raises {@link NumberFormatException}, so an unreadable query
 * parameter becomes a 404. On a listing route that status reads as "this catalog or namespace does
 * not exist", which sends the caller looking for the wrong problem.
 *
 * <p>The same sentence exempts a {@code WebApplicationException} from that wrapping and passes it
 * through as thrown. {@link BadRequestException} is one, which is why throwing it here reaches the
 * client as a 400 while a {@link NumberFormatException} cannot, whatever status the exception
 * mappers would otherwise give an {@code IllegalArgumentException}.
 *
 * <p>Only the boxed types are handled. No resource declares a primitive numeric parameter, so those
 * are left to the runtime.
 */
@Provider
public class NumericParamConverterProvider implements ParamConverterProvider {

  @Override
  @SuppressWarnings("unchecked")
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (rawType == Integer.class) {
      return (ParamConverter<T>) new NumericConverter<>(Integer::valueOf, nameOf(annotations));
    }
    if (rawType == Long.class) {
      return (ParamConverter<T>) new NumericConverter<>(Long::valueOf, nameOf(annotations));
    }
    return null;
  }

  /**
   * The name the caller used for this parameter, so the message can say which one was rejected, or
   * null when the parameter is not bound from the query string.
   */
  private static String nameOf(Annotation[] annotations) {
    for (Annotation annotation : annotations) {
      if (annotation instanceof QueryParam queryParam) {
        return queryParam.value();
      }
    }
    return null;
  }

  private static final class NumericConverter<T extends Number> implements ParamConverter<T> {

    private final Function<String, T> parser;
    private final String parameterName;

    private NumericConverter(Function<String, T> parser, String parameterName) {
      this.parser = parser;
      this.parameterName = parameterName;
    }

    @Override
    public T fromString(String value) {
      // A parameter the caller did not send, or sent empty, stays absent: presence is not this
      // converter's decision.
      if (value == null) {
        return null;
      }
      try {
        return parser.apply(value);
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            (parameterName == null ? "The parameter" : parameterName) + " must be an integer");
      }
    }

    @Override
    public String toString(T value) {
      return String.valueOf(value);
    }
  }
}
