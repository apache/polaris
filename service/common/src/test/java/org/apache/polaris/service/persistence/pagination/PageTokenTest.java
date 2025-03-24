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

package org.apache.polaris.service.persistence.pagination;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.polaris.core.persistence.pagination.OffsetPageToken;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.pagination.ReadEverythingPageToken;
import org.apache.polaris.jpa.models.ModelEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageTokenTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageTokenTest.class);

    static Stream<PageToken.PageTokenBuilder<?>> getPageTokenBuilders() {
        return Stream.of(
            OffsetPageToken.builder(), EntityIdPageToken.builder(), ReadEverythingPageToken.builder());
    }

    @Test
    void testDoneToken() {
        Assertions.assertThat(PageToken.DONE).isNull();
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testRoundTrips(PageToken.PageTokenBuilder<?> builder) {
        if (builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder) {
            // Skip ReadEverythingPageToken
            return;
        }

        for (int limit : List.of(1, 10, 100, Integer.MAX_VALUE)) {
            PageToken token = builder.fromLimit(limit);
            Assertions.assertThat(token.pageSize).isEqualTo(limit);
            Assertions.assertThat(builder.fromString(token.toString())).isEqualTo(token);
        }
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testInvalidLimits(PageToken.PageTokenBuilder<?> builder) {
        if (builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder) {
            // Skip ReadEverythingPageToken
            return;
        }

        for (int limit : List.of(-1, 0)) {
            Assertions.assertThatThrownBy(() -> builder.fromLimit(limit))
                .isInstanceOf(IllegalArgumentException.class);
        }

        Assertions.assertThat(builder.fromLimit(null)).isInstanceOf(ReadEverythingPageToken.class);
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testStartingTokens(PageToken.PageTokenBuilder<?> builder) {
        Assertions.assertThat(builder.fromString("")).isNotNull();
        if (!(builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder)) {
            Assertions.assertThat(builder.fromString("")).isNotEqualTo(ReadEverythingPageToken.get());
        }

        Assertions.assertThatThrownBy(() -> builder.fromString(null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testPageBuilding(PageToken.PageTokenBuilder<?> builder) {
        if (builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder) {
            // Skip ReadEverythingPageToken
            return;
        }

        List<ModelEntity> data =
            List.of(ModelEntity.builder().id(1).build(), ModelEntity.builder().id(2).build());

        PageToken token = builder.fromLimit(1000);
        Assertions.assertThat(token.buildNextPage(data).data).isEqualTo(data);
        Assertions.assertThat(token.buildNextPage(data).pageToken).isNull();
    }

    @Test
    void testUniquePrefixes() {
        Stream<PageToken.PageTokenBuilder<?>> builders = getPageTokenBuilders();
        List<String> prefixes =
            builders.map(PageToken.PageTokenBuilder::tokenPrefix).collect(Collectors.toList());
        Assertions.assertThat(prefixes.size()).isEqualTo(prefixes.stream().distinct().count());
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testCrossTokenParsing(PageToken.PageTokenBuilder<?> builder) {
        var otherBuilders = getPageTokenBuilders().collect(Collectors.toList());
        for (var otherBuilder : otherBuilders) {
            LOGGER.info(
                "Testing {} being parsed by {}",
                builder.getClass().getSimpleName(),
                otherBuilder.getClass().getSimpleName());

            final PageToken token;
            if (builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder) {
                token = ReadEverythingPageToken.get();
            } else {
                token = builder.fromLimit(1234);
            }
            if (otherBuilder.getClass().equals(builder.getClass())) {
                Assertions.assertThat(otherBuilder.fromString(token.toString())).isEqualTo(token);
            } else {
                Assertions.assertThatThrownBy(() -> otherBuilder.fromString(token.toString()))
                    .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getPageTokenBuilders")
    void testDefaultTokens(PageToken.PageTokenBuilder<?> builder) {
        if (builder instanceof ReadEverythingPageToken.ReadEverythingPageTokenBuilder) {
            // Skip ReadEverythingPageToken
            return;
        }

        PageToken token = builder.fromString("");
        Assertions.assertThat(token.toString()).isNotNull();
        Assertions.assertThat(token.pageSize).isEqualTo(PageToken.DEFAULT_PAGE_SIZE);
    }

    @Test
    void testReadEverythingPageToken() {
        PageToken token = ReadEverythingPageToken.get();

        Assertions.assertThat(token.toString()).isNotNull();
        Assertions.assertThat(token.updated(List.of("anything"))).isEqualTo(PageToken.DONE);
        Assertions.assertThat(token.pageSize).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testOffsetPageToken() {
        OffsetPageToken token = (OffsetPageToken) OffsetPageToken.builder().fromLimit(2);

        Assertions.assertThat(token).isInstanceOf(OffsetPageToken.class);
        Assertions.assertThat(token.offset).isEqualTo(0);

        List<String> data = List.of("some", "data");
        var page = token.buildNextPage(data);
        Assertions.assertThat(page.pageToken).isNotNull();
        Assertions.assertThat(page.pageToken).isInstanceOf(OffsetPageToken.class);
        Assertions.assertThat(page.pageToken.pageSize).isEqualTo(2);
        Assertions.assertThat(((OffsetPageToken) page.pageToken).offset).isEqualTo(2);
        Assertions.assertThat(page.data).isEqualTo(data);

        Assertions.assertThat(OffsetPageToken.builder().fromString(page.pageToken.toString()))
            .isEqualTo(page.pageToken);
    }

    @Test
    void testEntityIdPageToken() {
        EntityIdPageToken token = (EntityIdPageToken) EntityIdPageToken.builder().fromLimit(2);

        Assertions.assertThat(token).isInstanceOf(EntityIdPageToken.class);
        Assertions.assertThat(token.id).isEqualTo(-1L);

        List<String> badData = List.of("some", "data");
        Assertions.assertThatThrownBy(() -> token.buildNextPage(badData))
            .isInstanceOf(IllegalArgumentException.class);

        List<ModelEntity> data =
            List.of(ModelEntity.builder().id(101).build(), ModelEntity.builder().id(102).build());
        var page = token.buildNextPage(data);

        Assertions.assertThat(page.pageToken).isNotNull();
        Assertions.assertThat(page.pageToken).isInstanceOf(EntityIdPageToken.class);
        Assertions.assertThat(page.pageToken.pageSize).isEqualTo(2);
        Assertions.assertThat(((EntityIdPageToken) page.pageToken).id).isEqualTo(102);
        Assertions.assertThat(page.data).isEqualTo(data);

        Assertions.assertThat(EntityIdPageToken.builder().fromString(page.pageToken.toString()))
            .isEqualTo(page.pageToken);
    }
}
