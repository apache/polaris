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
package org.apache.polaris.persistence.api.obj;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
@JsonSerialize(as = ImmutableVersionedTestObj.class)
@JsonDeserialize(as = ImmutableVersionedTestObj.class)
public interface VersionedTestObj extends Obj {

  ObjType TYPE = new VersionedTestObjType();

  @Nonnull
  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableVersionedTestObj.Builder builder() {
    return ImmutableVersionedTestObj.builder();
  }

  @Nullable
  String someValue();

  @Nullable
  byte[] binary();

  final class VersionedTestObjType extends AbstractObjType<VersionedTestObj> {
    public VersionedTestObjType() {
      super("test-versioned", "versioned", VersionedTestObj.class);
    }
  }
}
