/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public final class InterceptingBucket extends Bucket {
  private final Bucket bucket;

  private volatile Function<String, Optional<MockObject>> retriever;
  private volatile Function<String, Optional<Boolean>> deleter;
  private volatile BiFunction<String, UpdaterMode, Optional<ObjectUpdater>> updater;
  private volatile BiFunction<String, String, Optional<Stream<ListElement>>> lister;

  public InterceptingBucket(Bucket bucket) {
    this.bucket = bucket;
    reset();
  }

  public void reset() {
    retriever = key -> Optional.empty();
    deleter = key -> Optional.empty();
    updater = (key, mode) -> Optional.empty();
    lister = (prefix, offset) -> Optional.empty();
  }

  public void setRetriever(Function<String, Optional<MockObject>> retriever) {
    this.retriever = retriever;
  }

  public void setDeleter(Function<String, Optional<Boolean>> deleter) {
    this.deleter = deleter;
  }

  public void setUpdater(BiFunction<String, UpdaterMode, Optional<ObjectUpdater>> updater) {
    this.updater = updater;
  }

  public void setLister(BiFunction<String, String, Optional<Stream<ListElement>>> lister) {
    this.lister = lister;
  }

  @Override
  public String creationDate() {
    return bucket.creationDate();
  }

  @Override
  public ObjectRetriever object() {
    return key -> retriever.apply(key).orElseGet(() -> bucket.object().retrieve(key));
  }

  @Override
  public Deleter deleter() {
    return key -> deleter.apply(key).orElseGet(() -> bucket.deleter().delete(key));
  }

  @Override
  public Updater updater() {
    return (key, mode) ->
        updater.apply(key, mode).orElseGet(() -> bucket.updater().update(key, mode));
  }

  @Override
  public Lister lister() {
    return (prefix, offset) ->
        lister.apply(prefix, offset).orElseGet(() -> bucket.lister().list(prefix, offset));
  }
}
