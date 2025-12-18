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

import static com.google.common.base.Preconditions.checkState;
import static java.net.NetworkInterface.networkInterfaces;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vert.x based address resolver.
 *
 * <p>Resolves names to both IPv4 and IPv6 addresses using a given search-list. These
 * functionalities are not supported via{@code InetAddress}.
 */
record AddressResolver(DnsClient dnsClient, List<String> searchList) {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddressResolver.class);

  /** Set of all locally bound IP addresses. */
  static final Set<String> LOCAL_ADDRESSES;

  private static final boolean IP_V4_ONLY;

  static {
    try {
      IP_V4_ONLY = Boolean.parseBoolean(System.getProperty("java.net.preferIPv4Stack", "false"));
      LOCAL_ADDRESSES =
          networkInterfaces()
              .flatMap(
                  ni ->
                      Stream.concat(
                              // localhost can be ipv6 when sysctl disable ipv6
                              // if ::1 is registered for localhost in /etc/hosts
                              // in this case java stack can still capture it
                              // and it will work (even if not great)
                              // this is a workaround to ensure at least localhost is
                              // in the list but this could be more general to all /etc/hosts
                              // mappings
                              IP_V4_ONLY || !"lo".equalsIgnoreCase(ni.getName())
                                  ? Stream.empty()
                                  : findLocalhostAddresses(),
                              ni.getInterfaceAddresses().stream()
                                  // Need to do this InetAddress->byte[]->InetAddress dance to get
                                  // rid of
                                  // host-address suffixes as in `0:0:0:0:0:0:0:1%lo`
                                  .map(InterfaceAddress::getAddress))
                          .map(InetAddress::getAddress)
                          .map(
                              a -> {
                                try {
                                  return InetAddress.getByAddress(a);
                                } catch (UnknownHostException e) {
                                  // Should never happen when calling getByAddress() with an IPv4 or
                                  // IPv6 address
                                  throw new RuntimeException(e);
                                }
                              })
                          .map(InetAddress::getHostAddress))
              .collect(toUnmodifiableSet());
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
  }

  private static @NonNull Stream<InetAddress> findLocalhostAddresses() {
    try {
      return Stream.of(InetAddress.getAllByName("localhost"));
    } catch (final RuntimeException | UnknownHostException e) {
      return Stream.empty();
    }
  }

  /**
   * Uses a "default" {@link DnsClient} using the first {@code nameserver} and the {@code search}
   * list configured in {@code /etc/resolv.conf}.
   */
  AddressResolver(Vertx vertx, long queryTimeoutMillis) {
    this(createDnsClient(vertx, queryTimeoutMillis), ResolvConf.system().getSearchList());
  }

  /**
   * Creates a "default" {@link DnsClient} using the first nameserver configured in {@code
   * /etc/resolv.conf}.
   */
  private static DnsClient createDnsClient(Vertx vertx, long queryTimeoutMillis) {
    var nameservers = ResolvConf.system().getNameservers();
    checkState(!nameservers.isEmpty(), "No nameserver configured in /etc/resolv.conf");
    var nameserver = nameservers.getFirst();
    LOGGER.info(
        "Using nameserver {}/{} with search list {}",
        nameserver.getHostName(),
        nameserver.getAddress().getHostAddress(),
        ResolvConf.system().getSearchList());
    return vertx.createDnsClient(
        new DnsClientOptions()
            // 5 seconds should be enough to resolve
            .setQueryTimeout(queryTimeoutMillis)
            .setHost(nameserver.getAddress().getHostAddress())
            .setPort(nameserver.getPort()));
  }

  private Future<List<String>> resolveSingle(String name) {
    var resultA = dnsClient.resolveA(name);
    if (IP_V4_ONLY) {
      return resultA;
    }
    return resultA.compose(
        a ->
            dnsClient
                .resolveAAAA(name)
                .map(aaaa -> Stream.concat(aaaa.stream(), a.stream()).collect(toList())));
  }

  /** Resolve a single name, used by {@link #resolveAll(List)}. */
  Future<List<String>> resolve(String name) {
    if (name.startsWith("=")) {
      return Future.succeededFuture(List.of(name.substring(1)));
    }

    // By convention, do not consult the 'search' list, when the name to query ends with a dot.
    var exact = name.endsWith(".");
    var query = exact ? name.substring(0, name.length() - 1) : name;
    var future = resolveSingle(query);
    if (!exact) {
      // Consult the 'search' list if the above 'resolveName' fails.
      for (var search : searchList) {
        future = future.recover(t -> resolveSingle(query + '.' + search));
      }
    }

    return future;
  }

  /** Resolve all names in parallel. */
  Future<List<String>> resolveAll(List<String> names) {
    var composite = Future.all(names.stream().map(this::resolve).collect(toList()));
    return composite.map(
        c ->
            IntStream.range(0, c.size())
                .mapToObj(c::resultAt)
                .map(
                    e -> {
                      @SuppressWarnings("unchecked")
                      var casted = (List<String>) e;
                      return casted.stream();
                    })
                .reduce(Stream::concat)
                .map(s -> s.collect(toList()))
                .orElse(List.of()));
  }
}
