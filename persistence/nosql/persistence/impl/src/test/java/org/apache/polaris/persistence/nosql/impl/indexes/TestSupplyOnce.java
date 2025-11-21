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

import static org.apache.polaris.persistence.nosql.impl.indexes.SupplyOnce.memoize;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSupplyOnce {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void nullValue() {
    AtomicInteger counter = new AtomicInteger();
    Supplier<String> nullValue =
        memoize(
            () -> {
              counter.incrementAndGet();
              return null;
            });

    soft.assertThat(counter).hasValue(0);

    soft.assertThat(nullValue.get()).isNull();
    soft.assertThat(counter).hasValue(1);
    soft.assertThat(nullValue.get()).isNull();
    soft.assertThat(counter).hasValue(1);
    soft.assertThat(nullValue.get()).isNull();
    soft.assertThat(counter).hasValue(1);
  }

  @Test
  public void someValue() {
    AtomicInteger counter = new AtomicInteger();
    Supplier<String> nullValue =
        memoize(
            () -> {
              counter.incrementAndGet();
              return "foo";
            });

    soft.assertThat(counter).hasValue(0);

    soft.assertThat(nullValue.get()).isEqualTo("foo");
    soft.assertThat(counter).hasValue(1);
    soft.assertThat(nullValue.get()).isEqualTo("foo");
    soft.assertThat(counter).hasValue(1);
    soft.assertThat(nullValue.get()).isEqualTo("foo");
    soft.assertThat(counter).hasValue(1);
  }

  @Test
  public void failure() {
    AtomicInteger counter = new AtomicInteger();
    Supplier<String> failure =
        memoize(
            () -> {
              counter.incrementAndGet();
              throw new RuntimeException("foo");
            });

    soft.assertThat(counter).hasValue(0);

    AtomicReference<RuntimeException> exceptionInstance = new AtomicReference<>();

    soft.assertThatRuntimeException()
        .isThrownBy(failure::get)
        .extracting(
            re -> {
              exceptionInstance.set(re);
              return re.getMessage();
            })
        .isEqualTo("foo");
    soft.assertThat(counter).hasValue(1);
    soft.assertThatRuntimeException().isThrownBy(failure::get).isSameAs(exceptionInstance.get());
    soft.assertThat(counter).hasValue(1);
    soft.assertThatRuntimeException().isThrownBy(failure::get).isSameAs(exceptionInstance.get());
    soft.assertThat(counter).hasValue(1);
  }
}
