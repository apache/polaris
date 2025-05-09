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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.AddressResolver.LOCAL_ADDRESSES;

import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestAddressResolver {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected Vertx vertx;
  AddressResolver addressResolver;

  @BeforeEach
  void setUp() {
    vertx = Vertx.builder().build();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (addressResolver != null) {
        addressResolver
            .dnsClient()
            .close()
            .toCompletionStage()
            .toCompletableFuture()
            .get(1, TimeUnit.MINUTES);
      }
    } finally {
      try {
        vertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
      } finally {
        vertx = null;
      }
    }
  }

  @Test
  public void resolveNoName() throws Exception {
    addressResolver = new AddressResolver(vertx);
    soft.assertThat(
            addressResolver
                .resolveAll(Collections.emptyList())
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isEmpty();
  }

  @Test
  public void resolveGoodName() throws Exception {
    addressResolver = new AddressResolver(vertx);

    AddressResolver addressResolverWithSearch =
        new AddressResolver(addressResolver.dnsClient(), List.of("org"));

    List<String> withoutSearchList =
        addressResolver
            .resolveAll(singletonList("projectnessie.org"))
            .toCompletionStage()
            .toCompletableFuture()
            .get(1, TimeUnit.MINUTES);
    soft.assertThat(withoutSearchList).isNotEmpty();

    List<String> withSearchList1 =
        addressResolverWithSearch
            .resolveAll(singletonList("projectnessie.org"))
            .toCompletionStage()
            .toCompletableFuture()
            .get(1, TimeUnit.MINUTES);
    soft.assertThat(withoutSearchList).isNotEmpty();
    soft.assertThat(withSearchList1).isNotEmpty().isNotEmpty();
    soft.assertThat(withSearchList1).containsExactlyInAnyOrderElementsOf(withoutSearchList);

    List<String> withSearchList2 =
        addressResolverWithSearch
            .resolveAll(singletonList("projectnessie"))
            .toCompletionStage()
            .toCompletableFuture()
            .get(1, TimeUnit.MINUTES);
    soft.assertThat(withSearchList2).isNotEmpty();
    soft.assertThat(withSearchList2).containsExactlyInAnyOrderElementsOf(withoutSearchList);

    List<String> withSearchListQualified =
        addressResolverWithSearch
            .resolveAll(singletonList("projectnessie.org."))
            .toCompletionStage()
            .toCompletableFuture()
            .get(1, TimeUnit.MINUTES);
    soft.assertThat(withSearchListQualified).isNotEmpty();

    soft.assertThat(withSearchListQualified).containsExactlyInAnyOrderElementsOf(withoutSearchList);
  }

  @Test
  @DisabledOnOs(value = OS.MAC, disabledReason = "Resolving 'localhost' doesn't work on macOS")
  public void resolveSingleName() throws Exception {
    addressResolver = new AddressResolver(vertx);
    soft.assertThat(
            addressResolver
                .resolveAll(singletonList("localhost"))
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isNotEmpty()
        .containsAnyOf("0:0:0:0:0:0:0:1", "127.0.0.1");
  }

  @Test
  public void resolveBadName() {
    addressResolver = new AddressResolver(vertx);
    soft.assertThat(
            addressResolver
                .resolveAll(singletonList("wepofkjeopiwkf.wepofkeowpkfpoew.weopfkewopfk.local"))
                .toCompletionStage()
                .toCompletableFuture())
        .failsWithin(1, TimeUnit.MINUTES)
        .withThrowableThat()
        .withCauseInstanceOf(DnsException.class);
  }

  @Test
  @DisabledOnOs(value = OS.MAC, disabledReason = "Resolving 'localhost' doesn't work on macOS")
  public void resolveFilterLocalAddresses() throws Exception {
    addressResolver = new AddressResolver(vertx);
    soft.assertThat(
            addressResolver
                .resolveAll(singletonList("localhost"))
                .map(
                    s -> s.stream().filter(adr -> !LOCAL_ADDRESSES.contains(adr)).collect(toList()))
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isEmpty();
  }
}
