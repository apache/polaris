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
package org.apache.polaris.maintenance.cel;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.cel.tools.ScriptCreateException;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCelReferenceContinuePredicate {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void invalidScript() {
    soft.assertThatRuntimeException()
        .isThrownBy(() -> CelReferenceContinuePredicate.createScript("invalidScript"))
        .withCauseInstanceOf(ScriptCreateException.class);
    soft.assertThatRuntimeException()
        .isThrownBy(() -> CelReferenceContinuePredicate.getScript("invalidScript"))
        .withCauseInstanceOf(ScriptCreateException.class);
  }

  @Test
  public void cacheWorks() {
    var s1 = CelReferenceContinuePredicate.getScript("true");
    var s2 = CelReferenceContinuePredicate.getScript("true");
    soft.assertThat(s1).isSameAs(s2);
    soft.assertThat(s1).isNotSameAs(CelReferenceContinuePredicate.getScript("false"));
  }

  @ParameterizedTest
  @MethodSource
  public void scripts(String script, boolean[] expected, Iterable<BaseCommitObj> commits) {
    var predicate =
        new CelReferenceContinuePredicate<>(
            "testRefName",
            (Function<BaseCommitObj, Duration>)
                o -> Duration.ofMillis(TimeUnit.MICROSECONDS.toMillis(o.createdAtMicros())),
            script);
    var iter = commits.iterator();
    for (int i = 0; i < expected.length; i++) {
      var exp = expected[i];
      soft.assertThat(predicate.test(iter.next()))
          .describedAs("test at commit %s", i + 1)
          .isEqualTo(exp);
    }
  }

  static Stream<Arguments> scripts() {
    var dummy = mock(BaseCommitObj.class);
    return Stream.of(
        arguments("false", new boolean[] {false, false, false}, List.of(dummy, dummy, dummy)),
        arguments(
            "ref == 'testRefName'", new boolean[] {true, true, true}, List.of(dummy, dummy, dummy)),
        arguments("commits < 3", new boolean[] {true, true, false}, List.of(dummy, dummy, dummy)),
        arguments(
            "ageMinutes < 100",
            new boolean[] {true, true, true, false},
            List.of(
                yieldAge(Duration.ofMinutes(0)),
                yieldAge(Duration.ofMinutes(1)),
                yieldAge(Duration.ofMinutes(99)),
                yieldAge(Duration.ofMinutes(100)))),
        arguments(
            "ageHours < 100",
            new boolean[] {true, true, true, false},
            List.of(
                yieldAge(Duration.ofHours(0)),
                yieldAge(Duration.ofHours(1)),
                yieldAge(Duration.ofHours(99)),
                yieldAge(Duration.ofHours(100)))),
        arguments(
            "ageDays < 100",
            new boolean[] {true, true, true, false},
            List.of(
                yieldAge(Duration.ofDays(0)),
                yieldAge(Duration.ofDays(1)),
                yieldAge(Duration.ofDays(99)),
                yieldAge(Duration.ofDays(100)))));
  }

  static BaseCommitObj yieldAge(Duration duration) {
    var m = mock(BaseCommitObj.class);
    when(m.createdAtMicros()).thenReturn(TimeUnit.MILLISECONDS.toMicros(duration.toMillis()));
    return m;
  }
}
