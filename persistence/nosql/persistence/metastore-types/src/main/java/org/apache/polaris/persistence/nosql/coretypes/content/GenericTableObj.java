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
package org.apache.polaris.persistence.nosql.coretypes.content;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutableGenericTableObj.class)
@JsonDeserialize(as = ImmutableGenericTableObj.class)
public interface GenericTableObj extends TableLikeObj {
  ObjType TYPE = new GenericTableObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> format();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> doc();

  static Builder builder() {
    return ImmutableGenericTableObj.builder();
  }

  final class GenericTableObjType extends AbstractObjType<GenericTableObj> {
    public GenericTableObjType() {
      super("gen-tab", "GenericTable", GenericTableObj.class);
    }
  }

  @SuppressWarnings("unused")
  interface Builder extends TableLikeObj.Builder<GenericTableObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(GenericTableObj from);

    @CanIgnoreReturnValue
    Builder format(String format);

    @CanIgnoreReturnValue
    Builder format(Optional<String> format);

    @CanIgnoreReturnValue
    Builder doc(String doc);

    @CanIgnoreReturnValue
    Builder doc(Optional<String> doc);
  }
}
