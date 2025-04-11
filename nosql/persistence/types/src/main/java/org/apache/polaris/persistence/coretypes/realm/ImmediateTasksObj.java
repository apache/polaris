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
package org.apache.polaris.persistence.coretypes.realm;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.api.obj.AbstractObjType;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.apache.polaris.persistence.coretypes.ContainerObj;

/**
 * Maintains all {@link ImmediateTaskObj}. The current version of this object is maintained via the
 * reference {@value #IMMEDIATE_TASKS_REF_NAME}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableImmediateTasksObj.class)
@JsonDeserialize(as = ImmutableImmediateTasksObj.class)
public interface ImmediateTasksObj extends ContainerObj {

  String IMMEDIATE_TASKS_REF_NAME = "immediate-tasks";

  ObjType TYPE = new ImmediateTasksObjType();

  static ImmutableImmediateTasksObj.Builder builder() {
    return ImmutableImmediateTasksObj.builder();
  }

  @Override
  default ObjType type() {
    return TYPE;
  }

  final class ImmediateTasksObjType extends AbstractObjType<ImmediateTasksObj> {
    public ImmediateTasksObjType() {
      super("itasks", "Immediate Tasks", ImmediateTasksObj.class);
    }
  }

  interface Builder extends ContainerObj.Builder<ImmediateTasksObj, Builder> {}
}
