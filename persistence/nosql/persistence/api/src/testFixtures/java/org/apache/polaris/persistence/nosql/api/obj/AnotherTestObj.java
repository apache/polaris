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
package org.apache.polaris.persistence.nosql.api.obj;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
@JsonSerialize(as = ImmutableAnotherTestObj.class)
@JsonDeserialize(as = ImmutableAnotherTestObj.class)
public interface AnotherTestObj extends Obj {

  ObjType TYPE = new AnotherTestObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  @Nullable
  String text();

  @Nullable
  byte[] binary();

  @Nullable
  Number number();

  @Nullable
  Map<String, String> map();

  @Nullable
  List<String> list();

  @Nullable
  Instant instant();

  Optional<String> optional();

  static ImmutableAnotherTestObj.Builder builder() {
    return ImmutableAnotherTestObj.builder();
  }

  final class AnotherTestObjType extends AbstractObjType<AnotherTestObj> {
    public AnotherTestObjType() {
      super("test-another", "another test obj type", AnotherTestObj.class);
    }
  }
}
