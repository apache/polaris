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
package tests.smallrye;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.polaris.docs.ConfigDocs.ConfigItem;
import org.apache.polaris.docs.ConfigDocs.ConfigPropertyName;

/** Documentation for {@code my.types}. */
@ConfigMapping(prefix = "my.types")
public interface AllTypes extends InterfaceOne {
  String string();

  Optional<String> optionalString();

  Duration duration();

  Optional<Duration> optionalDuration();

  Path path();

  Optional<Path> optionalPath();

  URI uri();

  Optional<URI> optionalUri();

  Instant instant();

  Optional<Instant> optionalInstant();

  List<String> stringList();

  @ConfigPropertyName("stringkey")
  Map<String, String> stringStringMap();

  @ConfigPropertyName("key2")
  Map<String, Duration> stringDurationMap();

  Map<String, Path> stringPathMap();

  OptionalInt optionalInt();

  OptionalLong optionalLong();

  OptionalDouble optionalDouble();

  Integer intBoxed();

  Long longBoxed();

  Double doubleBoxed();

  Float floatBoxed();

  int intPrim();

  long longPrim();

  double doublePrim();

  float floatPrim();

  Boolean boolBoxed();

  boolean boolPrim();

  SomeEnum enumThing();

  Optional<SomeEnum> optionalEnum();

  List<SomeEnum> listOfEnum();

  Map<String, SomeEnum> mapToEnum();

  Optional<Boolean> optionalBool();

  /** My {@code MappedA}. */
  MappedA mappedA();

  /** Optional {@code MappedA}. */
  Optional<MappedA> optionalMappedA();

  /** Map of string to {@code MappedA}. */
  @ConfigPropertyName("mappy")
  Map<String, MappedA> mapStringMappedA();

  /** Another map of string to {@code MappedA}, in its own section. */
  @ConfigItem(section = "Section A")
  @WithName("map.py")
  Map<String, MappedA> anotherMapStringMappedA();
}
