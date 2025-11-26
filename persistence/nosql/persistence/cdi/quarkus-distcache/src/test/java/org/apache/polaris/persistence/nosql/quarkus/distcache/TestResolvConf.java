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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestResolvConf {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void resolve(
      String resolvConfContent, List<InetSocketAddress> nameservers, List<String> searchList)
      throws Exception {
    ResolvConf resolvConf =
        ResolvConf.fromReader(new BufferedReader(new StringReader(resolvConfContent)));
    soft.assertThat(resolvConf)
        .extracting(ResolvConf::getNameservers, ResolvConf::getSearchList)
        .containsExactly(nameservers, searchList);
  }

  @Test
  public void system() throws IOException {
    String file = Files.readString(Paths.get("/etc/resolv.conf"));

    ResolvConf resolvConf = ResolvConf.system();
    soft.assertThat(resolvConf.getNameservers()).isNotEmpty();
    // This 'if' ensures that this test passes on the macOS test run in CI.
    if (file.contains("\nsearch ") || file.startsWith("search ")) {
      soft.assertThat(resolvConf.getSearchList()).isNotEmpty();
    } else {
      soft.assertThat(resolvConf.getSearchList()).isEmpty();
    }
  }

  static Stream<Arguments> resolve() {
    return Stream.of(
        arguments(
            """
            # See man:systemd-resolved.service(8) for details about the supported modes of
            # operation for /etc/resolv.conf.

            nameserver 127.0.0.1
            search search.domain
            """,
            List.of(new InetSocketAddress("127.0.0.1", 53)),
            List.of("search.domain")),
        arguments(
            """
            nameserver 127.0.0.1
            nameserver 1.2.3.4
            """,
            List.of(new InetSocketAddress("127.0.0.1", 53), new InetSocketAddress("1.2.3.4", 53)),
            List.of()),
        arguments(
            """
            nameserver 127.0.0.1
            nameserver 1.2.3.4
            search search.domain
            search anothersearch.anotherdomain
            """,
            List.of(new InetSocketAddress("127.0.0.1", 53), new InetSocketAddress("1.2.3.4", 53)),
            List.of("search.domain", "anothersearch.anotherdomain")),
        arguments(
            """
            nameserver 127.0.0.1
            nameserver 1.2.3.4
            search search.domain anothersearch.anotherdomain
            """,
            List.of(new InetSocketAddress("127.0.0.1", 53), new InetSocketAddress("1.2.3.4", 53)),
            List.of("search.domain", "anothersearch.anotherdomain")),
        arguments("", List.of(), List.of()));
  }
}
