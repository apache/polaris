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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ext.ParamConverter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class NumericParamConverterProviderTest {

  private final NumericParamConverterProvider provider = new NumericParamConverterProvider();

  /** Source of real parameter annotations, read by reflection rather than mocked. */
  @SuppressWarnings("unused")
  private void annotatedParameters(
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("limit") Long limit,
      @HeaderParam("X-Count") Integer headerCount,
      Integer unannotated) {}

  private static Annotation[] annotationsOf(int index) {
    for (Method method : NumericParamConverterProviderTest.class.getDeclaredMethods()) {
      if (method.getName().equals("annotatedParameters")) {
        return method.getParameterAnnotations()[index];
      }
    }
    throw new AssertionError("annotatedParameters not found");
  }

  private static final Annotation[] PAGE_SIZE = annotationsOf(0);
  private static final Annotation[] LIMIT = annotationsOf(1);
  private static final Annotation[] HEADER = annotationsOf(2);
  private static final Annotation[] NONE = annotationsOf(3);

  @Test
  public void testConverterIsOfferedForTheBoxedNumericTypes() {
    assertThat(provider.getConverter(Integer.class, Integer.class, PAGE_SIZE)).isNotNull();
    assertThat(provider.getConverter(Long.class, Long.class, LIMIT)).isNotNull();
  }

  @Test
  public void testConverterIsNotOfferedForOtherTypes() {
    assertThat(provider.getConverter(String.class, String.class, PAGE_SIZE)).isNull();
    assertThat(provider.getConverter(Boolean.class, Boolean.class, PAGE_SIZE)).isNull();
    assertThat(provider.getConverter(UUID.class, UUID.class, PAGE_SIZE)).isNull();
  }

  @Test
  public void testConverterIsNotOfferedForThePrimitiveNumericTypes() {
    // No resource declares one, so the runtime keeps them.
    assertThat(provider.getConverter(int.class, int.class, PAGE_SIZE)).isNull();
    assertThat(provider.getConverter(long.class, long.class, LIMIT)).isNull();
  }

  @Test
  public void testAReadableValueIsConverted() {
    assertThat(provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).fromString("5"))
        .isEqualTo(5);
    assertThat(provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).fromString("-3"))
        .isEqualTo(-3);
    assertThat(provider.getConverter(Long.class, Long.class, LIMIT).fromString("9000000000"))
        .isEqualTo(9000000000L);
  }

  @Test
  public void testAnAbsentValueStaysAbsent() {
    assertThat(provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).fromString(null))
        .isNull();
    assertThat(provider.getConverter(Long.class, Long.class, LIMIT).fromString(null)).isNull();
  }

  @Test
  public void testAnUnreadableValueIsRejectedByName() {
    assertThatThrownBy(
            () ->
                provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).fromString("large"))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("pageSize must be an integer");
    assertThatThrownBy(
            () -> provider.getConverter(Long.class, Long.class, LIMIT).fromString("plenty"))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("limit must be an integer");
  }

  @Test
  public void testAnEmptyValueWouldBeRejectedByName() {
    // The runtime binds an empty query value as null, so this case does not arise from the query
    // string; it is here so the converter's own contract is complete.
    assertThatThrownBy(
            () -> provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).fromString(""))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("pageSize must be an integer");
  }

  @Test
  public void testAnUnreadableValueWithoutAQueryParamNameIsRejectedGenerically() {
    assertThatThrownBy(
            () -> provider.getConverter(Integer.class, Integer.class, HEADER).fromString("large"))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("The parameter must be an integer");
    assertThatThrownBy(
            () -> provider.getConverter(Integer.class, Integer.class, NONE).fromString("large"))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("The parameter must be an integer");
  }

  @Test
  public void testTheRejectionCarriesBadRequest() {
    ParamConverter<Integer> converter =
        provider.getConverter(Integer.class, Integer.class, PAGE_SIZE);
    assertThatThrownBy(() -> converter.fromString("large"))
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(BadRequestException.class))
        .extracting(e -> e.getResponse().getStatus())
        .isEqualTo(400);
  }

  @Test
  public void testToStringRoundTripsTheValue() {
    assertThat(provider.getConverter(Integer.class, Integer.class, PAGE_SIZE).toString(7))
        .isEqualTo("7");
    assertThat(provider.getConverter(Long.class, Long.class, LIMIT).toString(7L)).isEqualTo("7");
  }
}
