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
@JsonSerialize(as = ImmutableDirectoryObj.class)
@JsonDeserialize(as = ImmutableDirectoryObj.class)
public interface DirectoryObj extends TableLikeObj {
  ObjType TYPE = new DirectoryObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> baseLocation();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> filterInclude();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> filterExclude();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> scanSchedule();

  static Builder builder() {
    return ImmutableDirectoryObj.builder();
  }

  final class DirectoryObjType extends AbstractObjType<DirectoryObj> {
    public DirectoryObjType() {
      super("directory", "Directory", DirectoryObj.class);
    }
  }

  @SuppressWarnings("unused")
  interface Builder extends TableLikeObj.Builder<DirectoryObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(DirectoryObj from);

    @CanIgnoreReturnValue
    Builder baseLocation(String baseLocation);

    @CanIgnoreReturnValue
    Builder baseLocation(Optional<String> baseLocation);

    @CanIgnoreReturnValue
    Builder filterInclude(String filterInclude);

    @CanIgnoreReturnValue
    Builder filterInclude(Optional<String> filterInclude);

    @CanIgnoreReturnValue
    Builder filterExclude(String filterExclude);

    @CanIgnoreReturnValue
    Builder filterExclude(Optional<String> filterExclude);

    @CanIgnoreReturnValue
    Builder scanSchedule(String scanSchedule);

    @CanIgnoreReturnValue
    Builder scanSchedule(Optional<String> scanSchedule);
  }
}
