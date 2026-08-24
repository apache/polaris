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

package org.apache.polaris.core.tag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;
import org.jspecify.annotations.Nullable;

/**
 * Pagination {@linkplain Token token} for tag reverse lookups over {@link TagAssignmentRecord}s.
 *
 * <p>Within one tag definition the remaining assignment identity is {@code (targetId, fieldId)}
 * (the target catalog is fixed by the same-catalog rule), so a page resumes strictly after that
 * composite key in {@code (target_id, field_id)} order.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableTagAssignmentTargetToken.class)
@JsonDeserialize(as = ImmutableTagAssignmentTargetToken.class)
@tools.jackson.databind.annotation.JsonSerialize(as = ImmutableTagAssignmentTargetToken.class)
@tools.jackson.databind.annotation.JsonDeserialize(as = ImmutableTagAssignmentTargetToken.class)
public interface TagAssignmentTargetToken extends Token {
  String ID = "ta";

  @JsonProperty("i")
  long targetId();

  @JsonProperty("f")
  int fieldId();

  @Override
  default String getT() {
    return ID;
  }

  static @Nullable TagAssignmentTargetToken fromRecord(@Nullable TagAssignmentRecord record) {
    if (record == null) {
      return null;
    }
    return ImmutableTagAssignmentTargetToken.builder()
        .targetId(record.getTargetId())
        .fieldId(record.getFieldId())
        .build();
  }

  final class TagAssignmentTargetTokenType implements TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return TagAssignmentTargetToken.class;
    }
  }
}
