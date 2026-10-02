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

package org.apache.polaris.core.persistence.pagination;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
class PageTokenTest {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void testReadEverything() {
    PageToken r = PageToken.readEverything();
    soft.assertThat(r.paginationRequested()).isFalse();
    soft.assertThat(r.pageSize()).isEmpty();
    soft.assertThat(r.value()).isEmpty();

    Page<Integer> pageEverything =
        Page.mapped(
            r,
            Stream.of(1, 2, 3, 4),
            Function.identity(),
            i -> i != null ? EntityIdToken.fromEntityId(i) : null);
    soft.assertThat(pageEverything.encodedResponseToken()).isNull();
    soft.assertThat(pageEverything.items()).containsExactly(1, 2, 3, 4);

    r = PageToken.build(null, null, -1, () -> true);
    soft.assertThat(r.paginationRequested()).isFalse();
    soft.assertThat(r.pageSize()).isEmpty();
    soft.assertThat(r.value()).isEmpty();
  }

  @Test
  public void testLimit() {
    PageToken r = PageToken.fromLimit(123);
    soft.assertThat(r).isEqualTo(PageToken.build(null, 123, -1, () -> true));
    soft.assertThat(r.paginationRequested()).isTrue();
    soft.assertThat(r.pageSize()).isEqualTo(OptionalInt.of(123));
    soft.assertThat(r.value()).isEmpty();
  }

  @Test
  public void testTokenValueForPaging() {
    PageToken r = PageToken.fromLimit(2);
    soft.assertThat(r).isEqualTo(PageToken.build(null, 2, -1, () -> true));
    Page<Integer> pageMoreData =
        Page.mapped(
            r,
            Stream.of(1, 2, 3, 4),
            Function.identity(),
            i -> i != null ? EntityIdToken.fromEntityId(i) : null);
    soft.assertThat(pageMoreData.encodedResponseToken()).isNotBlank();
    soft.assertThat(pageMoreData.items()).containsExactly(1, 2);

    // last page (no more data) - number of items is equal to the requested page size
    Page<Integer> lastPageSaturated =
        Page.mapped(
            r,
            Stream.of(3, 4),
            Function.identity(),
            i -> i != null ? EntityIdToken.fromEntityId(i) : null);
    // last page (no more data) -  next-token must be null
    soft.assertThat(lastPageSaturated.encodedResponseToken()).isNull();
    soft.assertThat(lastPageSaturated.items()).containsExactly(3, 4);

    // last page (no more data) - number of items is less than the requested page size
    Page<Integer> lastPageNotSaturated =
        Page.mapped(
            r,
            Stream.of(3),
            Function.identity(),
            i -> i != null ? EntityIdToken.fromEntityId(i) : null);
    soft.assertThat(lastPageNotSaturated.encodedResponseToken()).isNull();
    soft.assertThat(lastPageNotSaturated.items()).containsExactly(3);

    r = PageToken.fromLimit(200);
    soft.assertThat(r).isEqualTo(PageToken.build(null, 200, -1, () -> true));
    Page<Integer> page200 =
        Page.mapped(
            r,
            Stream.of(1, 2, 3, 4),
            Function.identity(),
            i -> i != null ? EntityIdToken.fromEntityId(i) : null);
    soft.assertThat(page200.encodedResponseToken()).isNull();
    soft.assertThat(page200.items()).containsExactly(1, 2, 3, 4);
  }

  @Test
  public void testInvalidPageSizeOnlyRejectedWhenPaginationIsEnabled() {
    soft.assertThatThrownBy(() -> PageToken.build(null, -1, -1, () -> true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid page size");

    // With pagination disabled the request is ignored wholesale, as it was before page size
    // bounding existed, so an invalid size must not start failing the request.
    soft.assertThat(PageToken.build(null, -1, -1, () -> false))
        .isEqualTo(PageToken.readEverything());
    soft.assertThat(PageToken.build(null, -1, 100, () -> false))
        .isEqualTo(PageToken.readEverything());
  }

  @Test
  public void testMaxPageSizeBoundsRequestedAndTokenEncodedSizes() {
    // A requested size above the maximum is reduced to it; one below is left alone.
    soft.assertThat(PageToken.build(null, 5000, 100, () -> true).pageSize()).hasValue(100);
    soft.assertThat(PageToken.build(null, 10, 100, () -> true).pageSize()).hasValue(10);

    // A token carries the size it was minted with, so that is bounded too.
    String oversized =
        PageTokenUtil.serializePageToken(
            ImmutablePageToken.builder()
                .pageSize(5000)
                .value(EntityIdToken.fromEntityId(42))
                .build());
    soft.assertThat(PageToken.build(oversized, null, 100, () -> true).pageSize()).hasValue(100);
  }

  @Test
  public void testMaxPageSizePaginatesWhenNothingIsRequested() {
    // With a maximum configured, an unsized request is paginated rather than reading everything.
    PageToken bounded = PageToken.build(null, null, 100, () -> true);
    soft.assertThat(bounded.paginationRequested()).isTrue();
    soft.assertThat(bounded.pageSize()).hasValue(100);

    // Without one, the Iceberg REST "read everything" behaviour is preserved.
    soft.assertThat(PageToken.build(null, null, -1, () -> true))
        .isEqualTo(PageToken.readEverything());
    soft.assertThat(PageToken.build(null, null, 0, () -> true))
        .isEqualTo(PageToken.readEverything());

    // An unlimited maximum never bounds an explicitly requested size either.
    soft.assertThat(PageToken.build(null, 5000, -1, () -> true).pageSize()).hasValue(5000);

    // Pagination disabled still reads everything, whatever the maximum.
    soft.assertThat(PageToken.build(null, 5000, 100, () -> false))
        .isEqualTo(PageToken.readEverything());
  }

  @ParameterizedTest
  @MethodSource
  public void testDeSer(Integer pageSize, String serializedPageToken, PageToken expectedPageToken) {
    soft.assertThat(PageTokenUtil.decodePageRequest(serializedPageToken, pageSize, -1, () -> true))
        .isEqualTo(expectedPageToken);
    soft.assertThat(PageTokenUtil.decodePageRequest(serializedPageToken, pageSize, -1, () -> false))
        .isEqualTo(PageToken.readEverything());
  }

  static Stream<Arguments> testDeSer() {
    var entity42page123 =
        ImmutablePageToken.builder().pageSize(123).value(EntityIdToken.fromEntityId(42)).build();
    var entity42page123ser = PageTokenUtil.serializePageToken(entity42page123);
    return Stream.of(
        arguments(null, null, PageToken.readEverything()),
        arguments(123, null, PageToken.fromLimit(123)),
        arguments(123, entity42page123ser, entity42page123),
        arguments(
            123,
            PageTokenUtil.serializePageToken(
                ImmutablePageToken.builder()
                    .pageSize(999999)
                    .value(EntityIdToken.fromEntityId(42))
                    .build()),
            entity42page123));
  }

  @ParameterizedTest
  @MethodSource
  public void testApiRoundTrip(Token token) {
    PageToken request = PageToken.build(null, 123, -1, () -> true);
    Page<?> page = Page.mapped(request, Stream.of("i1"), Function.identity(), x -> token);
    soft.assertThat(page.encodedResponseToken()).isNotBlank();

    PageToken r = PageToken.build(page.encodedResponseToken(), null, -1, () -> true);
    soft.assertThat(r.value()).contains(token);
    soft.assertThat(r.paginationRequested()).isTrue();
    soft.assertThat(r.pageSize()).isEqualTo(OptionalInt.of(123));

    r = PageToken.build(page.encodedResponseToken(), 456, -1, () -> true);
    soft.assertThat(r.value()).contains(token);
    soft.assertThat(r.paginationRequested()).isTrue();
    soft.assertThat(r.pageSize()).isEqualTo(OptionalInt.of(456));
  }

  static Stream<Token> testApiRoundTrip() {
    return Stream.of(
        EntityIdToken.fromEntityId(123),
        EntityIdToken.fromEntityId(456),
        ImmutableDummyTestToken.builder().s("str").i(42).build(),
        ImmutableDummyTestToken.builder().i(42).build(),
        ImmutableDummyTestToken.builder().build());
  }
}
