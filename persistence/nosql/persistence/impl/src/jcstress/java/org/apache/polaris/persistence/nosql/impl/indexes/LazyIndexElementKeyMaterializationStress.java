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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.impl.indexes.LazyIndexElementJCStressSupport.singleElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.LazyIndexElementJCStressSupport.value;

import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZ_Result;

@JCStressTest
@Outcome(
    id = "true, true",
    expect = Expect.ACCEPTABLE,
    desc = "Concurrent lazy key materialization returns the expected key.")
@State
public class LazyIndexElementKeyMaterializationStress {
  private static final IndexKey EXPECTED_KEY = key("key-a");

  private final InternalIndexElement<ObjRef> element = singleElement(EXPECTED_KEY, value());

  @Actor
  public void actor1(ZZ_Result r) {
    r.r1 = EXPECTED_KEY.equals(element.key());
  }

  @Actor
  public void actor2(ZZ_Result r) {
    r.r2 = EXPECTED_KEY.equals(element.key());
  }
}
