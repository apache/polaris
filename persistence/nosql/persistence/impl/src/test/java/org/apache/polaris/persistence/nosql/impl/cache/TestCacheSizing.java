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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.cache.CacheSizing.DEFAULT_HEAP_FRACTION;

import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheSizing {
  static final long BYTES_1G = 1024 * 1024 * 1024;
  static final long BYTES_512M = 512 * 1024 * 1024;
  static final long BYTES_4G = 4L * 1024 * 1024 * 1024;
  static final long BYTES_256M = 256 * 1024 * 1024;
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void illegalFractionSettings() {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fractionOfMaxHeapSize(-.1d).build());
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fractionOfMaxHeapSize(1.1d).build());
  }

  @Test
  void illegalFixedSettings() {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fixedSize(MemorySize.ofMega(-1)).build());
  }

  @Test
  void fixedSizeWins() {
    var fixedSize = MemorySize.ofMega(3);
    soft.assertThat(
            CacheSizing.builder()
                .fixedSize(fixedSize)
                .fractionOfMaxHeapSize(.5)
                .build()
                .calculateEffectiveSize(BYTES_512M, DEFAULT_HEAP_FRACTION))
        .isEqualTo(fixedSize.asLong());
  }

  @Test
  void tinyHeap() {
    // Assuming a 256MB max heap, requesting 70% (358MB), calc yields 64MB (min-size)
    var fractionMinSize = MemorySize.ofMega(64);
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .fractionMinSize(fractionMinSize)
                .build()
                .calculateEffectiveSize(BYTES_256M, DEFAULT_HEAP_FRACTION))
        .isEqualTo(fractionMinSize.asLong());
  }

  @Test
  void tinyHeapNoCache() {
    // Assuming a 256MB max heap, requesting 70% (179MB), calc yields fractionMinSizeMb, i.e. zero
    var fractionMinSize = MemorySize.ofMega(0);
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .fractionMinSize(fractionMinSize)
                .build()
                .calculateEffectiveSize(BYTES_256M, DEFAULT_HEAP_FRACTION))
        .isEqualTo(fractionMinSize.asLong());
  }

  @Test
  void defaultSettings4G() {
    // Assuming a 4G max heap, requesting 70% (358MB), sizing must yield 2867MB.
    soft.assertThat(
            CacheSizing.builder().build().calculateEffectiveSize(BYTES_4G, DEFAULT_HEAP_FRACTION))
        .isEqualTo(2576980377L);
  }

  @Test
  void defaultSettings1G() {
    soft.assertThat(
            CacheSizing.builder().build().calculateEffectiveSize(BYTES_1G, DEFAULT_HEAP_FRACTION))
        // 70 % of 1024 MB
        .isEqualTo(644245094L);
  }

  @Test
  void defaultSettingsTiny() {
    soft.assertThat(
            CacheSizing.builder().build().calculateEffectiveSize(BYTES_256M, DEFAULT_HEAP_FRACTION))
        // 70 % of 1024 MB
        .isEqualTo(MemorySize.ofMega(64).asLong());
  }

  @Test
  void turnOff() {
    soft.assertThat(
            CacheSizing.builder()
                .fixedSize(MemorySize.ofMega(0))
                .build()
                .calculateEffectiveSize(BYTES_1G, DEFAULT_HEAP_FRACTION))
        // 70 % of 1024 MB
        .isEqualTo(MemorySize.ofMega(0).asLong());
  }

  @Test
  void keepsHeapFree() {
    // Assuming a 512MB max heap, requesting 70% (358MB), exceeds "min free" of 256MB, sizing must
    // yield 256MB.
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .build()
                .calculateEffectiveSize(BYTES_512M, DEFAULT_HEAP_FRACTION))
        .isEqualTo(MemorySize.ofMega(256).asLong());
  }
}
