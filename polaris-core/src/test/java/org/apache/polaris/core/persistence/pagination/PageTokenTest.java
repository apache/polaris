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

import static org.assertj.core.api.Assertions.*;

import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PageTokenTest {

  @Test
  public void testReadEverything() {
    PageToken r = PageToken.readEverything();
    assertThat(r.paginationRequested()).isFalse();
    assertThatThrownBy(r::encodedDataReference).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(r::pageSize).isInstanceOf(IllegalStateException.class);

    r = PageToken.build(null, null);
    assertThat(r.paginationRequested()).isFalse();
    assertThatThrownBy(r::encodedDataReference).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(r::pageSize).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testLimit() {
    PageToken r = PageToken.fromLimit(123);
    assertThat(r.encodedDataReference()).isNull();
    assertThat(r.paginationRequested()).isTrue();
    assertThat(r.pageSize()).isEqualTo(123);
  }

  @Test
  public void testFirstRequest() {
    PageToken r = PageToken.build(null, 123);
    assertThat(r.encodedDataReference()).isNull();
    assertThat(r.paginationRequested()).isTrue();
    assertThat(r.pageSize()).isEqualTo(123);
  }

  @Test
  public void testApiRoundTrip() {
    PageToken request = PageToken.build(null, 123);
    Page<?> page = Page.mapped(request, Stream.of("i1"), Function.identity(), x -> "test:123");
    assertThat(page.encodedResponseToken()).isNotNull();
    PageToken r = PageToken.build(page.encodedResponseToken(), null);
    assertThat(r.paginationRequested()).isTrue();
    assertThat(r.encodedDataReference()).isEqualTo("test:123");
    assertThat(r.pageSize()).isEqualTo(123);

    r = PageToken.build(page.encodedResponseToken(), 456);
    assertThat(r.paginationRequested()).isTrue();
    assertThat(r.encodedDataReference()).isEqualTo("test:123");
    assertThat(r.pageSize()).isEqualTo(456);
  }
}
