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
package org.apache.polaris.ids.impl;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;
import static org.openjdk.jcstress.annotations.Expect.UNKNOWN;

import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.Z_Result;

public class SnowflakeIdGeneratorStress {
  public static final MonotonicClockImpl CLOCK = new MonotonicClockImpl().start();
  public static final IdGenerator IDGEN =
      new SnowflakeIdGeneratorImpl(
          new IdGeneratorSource() {
            @Override
            public int nodeId() {
              return 42;
            }

            @Override
            public long currentTimeMillis() {
              return CLOCK.currentTimeMillis();
            }
          });

  @JCStressTest
  @Description("Verify that generated IDs are unique for the same thread.")
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = ACCEPTABLE, desc = "Not equal, greater"),
    @Outcome(id = "-1", expect = FORBIDDEN, desc = "Not equal, smaller"),
    @Outcome(id = "0", expect = FORBIDDEN, desc = "Equal"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class SameThread {
    @Actor
    public void actor(I_Result r) {
      var v1 = IDGEN.generateId();
      var v2 = IDGEN.generateId();

      r.r1 = Long.compare(v2, v1);
    }
  }

  @JCStressTest
  @Description("Verify that generated IDs are unique for the same thread.")
  @Outcome.Outcomes({
    @Outcome(id = "false", expect = ACCEPTABLE, desc = "Not equal"),
    @Outcome(id = "true", expect = FORBIDDEN, desc = "Equal"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class DifferentThreads {
    long v1;
    long v2;

    @Actor
    public void actor1() {
      v1 = IDGEN.generateId();
    }

    @Actor
    public void actor2() {
      v1 = IDGEN.generateId();
    }

    @Arbiter
    public void arbiter(Z_Result r) {
      r.r1 = v1 == v2;
    }
  }
}
