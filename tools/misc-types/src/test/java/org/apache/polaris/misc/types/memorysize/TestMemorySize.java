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
package org.apache.polaris.misc.types.memorysize;

import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.annotation.Nullable;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.immutables.PolarisImmutable;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMemorySize {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void parseString(String in, String expected) {
    soft.assertThat(requireNonNull(MemorySize.valueOf(in)).toString()).isEqualTo(expected);
  }

  static Stream<Arguments> parseString() {
    return Stream.of(
        arguments("0G", "0B"),
        arguments("1024M", "1G"),
        arguments(String.valueOf(1024 * 1024), "1M"),
        arguments(String.valueOf(4 * 1024 * 1024), "4M"),
        arguments(String.valueOf(1024 * 1024 * 1024), "1G"));
  }

  @ParameterizedTest
  @MethodSource
  public void parse(String s, BigInteger expected) {
    var parsed = requireNonNull(MemorySize.valueOf(s));
    soft.assertThat(parsed.asBigInteger()).isEqualTo(expected);
    if (Character.isDigit(s.charAt(s.length() - 1))) {
      soft.assertThat(parsed.toString()).isEqualTo(s.toUpperCase(ROOT) + 'B');
    } else {
      soft.assertThat(parsed.toString()).isEqualTo(s.toUpperCase(ROOT));
    }
    try {
      var l = expected.longValueExact();
      soft.assertThat(parsed.asLong()).isEqualTo(l);
      soft.assertThat(parsed).isInstanceOf(MemorySize.MemorySizeLong.class);
    } catch (ArithmeticException e) {
      soft.assertThat(parsed).isInstanceOf(MemorySize.MemorySizeBig.class);
    }
  }

  static Stream<Arguments> parse() {
    return Stream.of(
            tuple("", BigInteger.ONE),
            tuple("B", BigInteger.ONE),
            tuple("K", BigInteger.valueOf(1024)),
            tuple("M", BigInteger.valueOf(1024).pow(2)),
            tuple("G", BigInteger.valueOf(1024).pow(3)),
            tuple("T", BigInteger.valueOf(1024).pow(4)),
            tuple("P", BigInteger.valueOf(1024).pow(5)),
            tuple("E", BigInteger.valueOf(1024).pow(6)),
            tuple("Z", BigInteger.valueOf(1024).pow(7)),
            tuple("Y", BigInteger.valueOf(1024).pow(8)))
        .flatMap(
            t ->
                Stream.of(
                    tuple(t.toList().get(0).toString().toLowerCase(ROOT), t.toList().get(1)), t))
        .flatMap(
            t -> {
              var suffix = t.toList().get(0).toString();
              var mult = (BigInteger) t.toList().get(1);
              return Stream.of(
                  arguments("1" + suffix, mult),
                  arguments("5" + suffix, mult.multiply(BigInteger.valueOf(5))),
                  arguments("32" + suffix, mult.multiply(BigInteger.valueOf(32))),
                  arguments("1023" + suffix, mult.multiply(BigInteger.valueOf(1023))));
            });
  }

  @ParameterizedTest
  @MethodSource("parse")
  public void serdeConfig(String input, BigInteger expected) {
    var configMap =
        Map.of(
            "memory-size.implicit", input,
            "memory-size.optional-present", input);
    var config =
        new SmallRyeConfigBuilder()
            .withMapping(MemorySizeConfig.class)
            .addDiscoveredConverters()
            .withSources(new PropertiesConfigSource(configMap, "configMap"))
            .build()
            .getConfigMapping(MemorySizeConfig.class);
    var value = new MemorySize.MemorySizeBig(expected);
    soft.assertThat(config)
        .extracting(
            MemorySizeConfig::implicit,
            MemorySizeConfig::optionalEmpty,
            MemorySizeConfig::optionalPresent)
        .containsExactly(value, Optional.empty(), Optional.of(value));
  }

  @ConfigMapping(prefix = "memory-size")
  interface MemorySizeConfig {
    MemorySize implicit();

    Optional<MemorySize> optionalEmpty();

    Optional<MemorySize> optionalPresent();
  }

  @ParameterizedTest
  @MethodSource("parse")
  public void serdeJackson(@SuppressWarnings("unused") String input, BigInteger expected)
      throws Exception {
    var value = new MemorySize.MemorySizeBig(expected);

    var immutable =
        ImmutableMemorySizeJson.builder()
            .implicit(value)
            .implicitInt(value)
            .optionalPresent(value)
            .optionalPresentInt(value)
            .build();

    var mapper = new ObjectMapper().findAndRegisterModules();
    var json = mapper.writeValueAsString(immutable);
    var nodes = mapper.readValue(json, JsonNode.class);
    var deser = mapper.readValue(json, MemorySizeJson.class);

    soft.assertThat(deser).isEqualTo(immutable);

    soft.assertThat(nodes.get("implicit").asText()).isEqualTo(value.toString());
    soft.assertThat(nodes.get("implicitInt").bigIntegerValue()).isEqualTo(value.asBigInteger());
    soft.assertThat(nodes.get("optionalPresent").asText()).isEqualTo(value.toString());
    soft.assertThat(nodes.get("optionalPresentInt").bigIntegerValue())
        .isEqualTo(value.asBigInteger());
  }

  @PolarisImmutable
  @JsonSerialize(as = ImmutableMemorySizeJson.class)
  @JsonDeserialize(as = ImmutableMemorySizeJson.class)
  interface MemorySizeJson {
    MemorySize implicit();

    @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
    MemorySize implicitInt();

    @Nullable
    MemorySize implicitNull();

    @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
    @Nullable
    MemorySize implicitIntNull();

    Optional<MemorySize> optionalEmpty();

    Optional<MemorySize> optionalPresent();

    @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
    Optional<MemorySize> optionalPresentInt();
  }
}
