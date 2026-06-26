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

package org.apache.polaris.persistence.nosql.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class TestStreamUtil {

  @Test
  public void bucketizedFetchesBucketsInOrder() {
    var seenBuckets = new ArrayList<List<Integer>>();

    var result =
        StreamUtil.bucketized(
                IntStream.range(0, 7).boxed(),
                bucket -> {
                  seenBuckets.add(List.copyOf(bucket));
                  return bucket.stream().map(value -> value * 10).toList();
                },
                3)
            .toList();

    assertThat(seenBuckets).containsExactly(List.of(0, 1, 2), List.of(3, 4, 5), List.of(6));
    assertThat(result).containsExactly(0, 10, 20, 30, 40, 50, 60);
  }

  @Test
  public void bucketizedRejectsNonPositiveBucketSize() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> StreamUtil.bucketized(IntStream.range(0, 1).boxed(), List::copyOf, 0))
        .withMessage("bucketSize must be positive");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> StreamUtil.bucketized(IntStream.range(0, 1).boxed(), List::copyOf, -1))
        .withMessage("bucketSize must be positive");
  }
}
