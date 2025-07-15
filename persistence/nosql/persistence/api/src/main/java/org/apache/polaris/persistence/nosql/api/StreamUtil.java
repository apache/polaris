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

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class StreamUtil {
  /**
   * Bucketizes the elements of the source stream, passes each bucket through the {@code
   * bucketFetcher} function, eventually yielding a stream the elements of all buckets.
   *
   * <p>A classic use case for this function is {@link Persistence#bucketizedBulkFetches(Stream,
   * Class)}.
   */
  public static <S, R> Stream<R> bucketized(
      Stream<S> source, Function<List<S>, List<R>> bucketFetcher, int bucketSize) {
    var sourceIter = source.iterator();

    var split =
        new Spliterator<List<R>>() {
          @Override
          public boolean tryAdvance(Consumer<? super List<R>> action) {
            if (!sourceIter.hasNext()) {
              // nothing more to do
              return false;
            }

            var bucket = new ArrayList<S>(bucketSize);
            for (int i = 0; i < bucketSize && sourceIter.hasNext(); i++) {
              bucket.add(sourceIter.next());
            }
            var fetched = bucketFetcher.apply(bucket);
            action.accept(fetched);

            return true;
          }

          @Override
          public Spliterator<List<R>> trySplit() {
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return 0;
          }
        };
    return StreamSupport.stream(split, false).flatMap(List::stream);
  }
}
