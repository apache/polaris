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

import org.apache.polaris.core.persistence.pagination.DonePageToken;
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
}
