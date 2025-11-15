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
package org.apache.polaris.persistence.nosql.coretypes.catalog;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;

@PolarisImmutable
@JsonSerialize(as = ImmutableFileObj.class)
@JsonDeserialize(as = ImmutableFileObj.class)
public interface FileObj extends ObjBase {
  ObjType TYPE = new FileObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static Builder builder() {
    return ImmutableFileObj.builder();
  }

  final class FileObjType extends AbstractObjType<FileObj> {
    public FileObjType() {
      super("file", "File", FileObj.class);
    }
  }

  interface Builder extends ObjBase.Builder<FileObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(FileObj from);
  }
}
