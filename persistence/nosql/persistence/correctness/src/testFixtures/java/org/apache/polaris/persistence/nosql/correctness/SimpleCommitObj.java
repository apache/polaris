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
package org.apache.polaris.persistence.nosql.correctness;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutableSimpleCommitObj.class)
@JsonDeserialize(as = ImmutableSimpleCommitObj.class)
public interface SimpleCommitObj extends BaseCommitObj {
  ObjType TYPE = new SimpleCommitObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  /**
   * Record the commit-numbers of all threads in a list, indexed by thread-number. Using a list here
   * allows the correctness checks to work even if the database driver reported an {@linkplain
   * UnknownOperationResultException unknown operation result}.
   */
  List<Integer> commitSeqPerThread();

  int thread();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  IndexContainer<ObjRef> index();

  final class SimpleCommitObjType extends AbstractObjType<SimpleCommitObj> {
    public SimpleCommitObjType() {
      super("s-c", "simple commit", SimpleCommitObj.class);
    }
  }

  interface Builder extends BaseCommitObj.Builder<SimpleCommitObj, Builder> {}
}
