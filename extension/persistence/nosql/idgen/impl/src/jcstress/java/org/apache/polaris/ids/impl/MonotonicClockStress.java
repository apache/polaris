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

import java.time.Instant;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

public class MonotonicClockStress {
  public static final MonotonicClockImpl CLOCK = new MonotonicClockImpl().start();

  @JCStressTest
  @Description("Verify that monotonicity is guaranteed across different threads (nanos).")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Both newer"),
    @Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "Newer + same time"),
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Same time + newer"),
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Both same time"),
    @Outcome(id = "-1, .*", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(id = ".*, -1", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class Nanos {
    long ref;

    long v1;
    long v2;

    public Nanos() {
      ref = CLOCK.nanoTime();
    }

    @Actor
    public void actor1() {
      v1 = CLOCK.nanoTime();
    }

    @Actor
    public void actor2() {
      v2 = CLOCK.nanoTime();
    }

    @Arbiter
    public void arbiter(II_Result r) {
      r.r1 = Long.compare(v1, ref);
      r.r2 = Long.compare(v2, ref);
    }
  }

  @JCStressTest
  @Description("Verify that monotonicity is guaranteed across different threads (micros).")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Both newer"),
    @Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "Newer + same time"),
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Same time + newer"),
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Both same time"),
    @Outcome(id = "-1, .*", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(id = ".*, -1", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class Micros {
    long ref;

    long v1;
    long v2;

    public Micros() {
      ref = CLOCK.currentTimeMicros();
    }

    @Actor
    public void actor1() {
      v1 = CLOCK.currentTimeMicros();
    }

    @Actor
    public void actor2() {
      v2 = CLOCK.currentTimeMicros();
    }

    @Arbiter
    public void arbiter(II_Result r) {
      r.r1 = Long.compare(v1, ref);
      r.r2 = Long.compare(v2, ref);
    }
  }

  @JCStressTest
  @Description("Verify that monotonicity is guaranteed across different threads (millis).")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Both newer"),
    @Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "Newer + same time"),
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Same time + newer"),
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Both same time"),
    @Outcome(id = "-1, .*", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(id = ".*, -1", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class Millis {
    long ref;

    long v1;
    long v2;

    public Millis() {
      ref = CLOCK.currentTimeMillis();
    }

    @Actor
    public void actor1() {
      v1 = CLOCK.currentTimeMillis();
    }

    @Actor
    public void actor2() {
      v2 = CLOCK.currentTimeMillis();
    }

    @Arbiter
    public void arbiter(II_Result r) {
      r.r1 = Long.compare(v1, ref);
      r.r2 = Long.compare(v2, ref);
    }
  }

  @JCStressTest
  @Description("Verify that monotonicity is guaranteed across different threads (instants).")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Both newer"),
    @Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "Newer + same time"),
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Same time + newer"),
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Both same time"),
    @Outcome(id = "-1, .*", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(id = ".*, -1", expect = FORBIDDEN, desc = "Clock must not go backwards"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class Instants {
    Instant ref;

    Instant v1;
    Instant v2;

    public Instants() {
      ref = CLOCK.currentInstant();
    }

    @Actor
    public void actor1() {
      v1 = CLOCK.currentInstant();
    }

    @Actor
    public void actor2() {
      v2 = CLOCK.currentInstant();
    }

    @Arbiter
    public void arbiter(II_Result r) {
      r.r1 = Integer.compare(v1.compareTo(ref), 0);
      r.r2 = Integer.compare(v2.compareTo(ref), 0);
    }
  }
}
