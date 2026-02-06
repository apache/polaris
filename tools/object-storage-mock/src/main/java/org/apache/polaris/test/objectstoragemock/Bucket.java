/*
 * Copyright (C) 2022 Dremio
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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.InputStream;
import java.time.Instant;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Bucket {

  public static ImmutableBucket.Builder builder() {
    return ImmutableBucket.builder();
  }

  @Value.Default
  public String creationDate() {
    return Instant.now().toString();
  }

  @Value.Default
  public ObjectRetriever object() {
    return x -> null;
  }

  @Value.Default
  public Deleter deleter() {
    return x -> false;
  }

  @Value.Default
  public Lister lister() {
    return (String prefix, String offset) -> Stream.empty();
  }

  @Value.Default
  public Updater updater() {
    return (objectName, mode) -> {
      throw new UnsupportedOperationException();
    };
  }

  @FunctionalInterface
  public interface Deleter {
    boolean delete(String key);
  }

  @FunctionalInterface
  public interface Updater {
    ObjectUpdater update(String key, UpdaterMode mode);
  }

  public enum UpdaterMode {
    CREATE_NEW,
    UPDATE,
    UPSERT,
  }

  public interface ObjectUpdater {
    @CanIgnoreReturnValue
    ObjectUpdater append(long position, InputStream data);

    @CanIgnoreReturnValue
    ObjectUpdater flush();

    @CanIgnoreReturnValue
    ObjectUpdater setContentType(String contentType);

    @CanIgnoreReturnValue
    MockObject commit();
  }

  @FunctionalInterface
  public interface Lister {
    Stream<ListElement> list(String prefix, String offset);
  }

  public interface ListElement {
    String key();

    MockObject object();
  }
}
