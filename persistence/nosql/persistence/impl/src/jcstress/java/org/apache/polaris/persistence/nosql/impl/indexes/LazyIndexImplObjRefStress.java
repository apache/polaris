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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static org.apache.polaris.persistence.nosql.impl.indexes.LazyIndexImplJCStressSupport.ASSIGNED_REF;
import static org.apache.polaris.persistence.nosql.impl.indexes.LazyIndexImplJCStressSupport.lazyIndex;

import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZI_Result;

@JCStressTest
@Outcome(
    id = "true, true, 1",
    expect = Expect.ACCEPTABLE,
    desc = "objRef assignment is visible on both wrapper and loaded delegate.")
@State
public class LazyIndexImplObjRefStress {
  private final LazyIndexImplJCStressSupport.CountingSupplier supplier =
      new LazyIndexImplJCStressSupport.CountingSupplier();
  private final IndexSpi<ObjRef> index = lazyIndex(supplier);

  @Actor
  public void actor1() {
    index.setObjId(ASSIGNED_REF);
  }

  @Actor
  public void actor2() {
    index.asKeyList();
  }

  @Arbiter
  public void arbiter(ZZI_Result r) {
    r.r1 = ASSIGNED_REF.equals(index.getObjId());
    r.r2 = ASSIGNED_REF.equals(supplier.delegate.getObjId());
    r.r3 = supplier.calls.get();
  }
}
