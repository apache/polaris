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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.pagination.DonePageToken;
import org.apache.polaris.core.persistence.pagination.EntityIdPageToken;
import org.apache.polaris.core.persistence.pagination.HasPageSize;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageTokenTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageTokenTest.class);

  @Test
  void testDoneToken() {
    Assertions.assertThat(new DonePageToken()).doesNotReturn(null, PageToken::toString);
    Assertions.assertThat(new DonePageToken()).returns(null, PageToken::toTokenString);
    Assertions.assertThat(new DonePageToken()).isEqualTo(new DonePageToken());
    Assertions.assertThat(new DonePageToken().hashCode()).isEqualTo(new DonePageToken().hashCode());
  }

  @Test
  void testReadEverythingPageToken() {
    PageToken token = PageToken.readEverything();

    Assertions.assertThat(token.toString()).isNotNull();
    Assertions.assertThat(token.toTokenString()).isNotNull();
    Assertions.assertThat(token).isNotInstanceOf(HasPageSize.class);

    Assertions.assertThat(PageToken.readEverything()).isEqualTo(PageToken.readEverything());
  }

  @Test
  void testEntityIdPageToken() {
    EntityIdPageToken token = new EntityIdPageToken(2);

    Assertions.assertThat(token).isInstanceOf(EntityIdPageToken.class);
    Assertions.assertThat(token.getId()).isEqualTo(-1L);

    // EntityIdPageToken can only build a new page from certain types that have an Entity ID
    List<String> badData = List.of("some", "data");
    Assertions.assertThatThrownBy(() -> token.buildNextPage(badData))
        .isInstanceOf(IllegalStateException.class);

    List<PolarisBaseEntity> data =
        List.of(
            new PolarisBaseEntity(
                0, 101, PolarisEntityType.NULL_TYPE, PolarisEntitySubType.ANY_SUBTYPE, 0, "101"),
            new PolarisBaseEntity(
                0, 102, PolarisEntityType.NULL_TYPE, PolarisEntitySubType.ANY_SUBTYPE, 0, "102"));
    var page = token.buildNextPage(data);

    Assertions.assertThat(page.pageToken).isNotNull();
    Assertions.assertThat(page.pageToken).isInstanceOf(EntityIdPageToken.class);
    Assertions.assertThat(((EntityIdPageToken) page.pageToken).getPageSize()).isEqualTo(2);
    Assertions.assertThat(((EntityIdPageToken) page.pageToken).getId()).isEqualTo(102);
    Assertions.assertThat(page.items).isEqualTo(data);

    Assertions.assertThat(PageToken.fromString(page.pageToken.toTokenString()))
        .isEqualTo(page.pageToken);
  }

  @Test
  void testInvalidPageTokens() {
    Assertions.assertThatCode(() -> PageToken.fromString("not-real"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unrecognized page token");

    PageToken goodToken = PageToken.fromLimit(100);
    Assertions.assertThatCode(() -> PageToken.fromString(goodToken.toTokenString() + "???"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid token format");

    Assertions.assertThatCode(() -> PageToken.fromString(EntityIdPageToken.PREFIX + "/1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid token format");
  }
}
