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

// Code mostly copied from io.netty.resolver.dns.ResolvConf, but with the addition to extract
// the 'search' option values.
//
// Marker for Polaris LICENSE file - keep it
// CODE_COPIED_TO_POLARIS

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Looks up the {@code nameserver}s and {@code search} domains from the {@code /etc/resolv.conf}
 * file, intended for Linux and macOS.
 */
final class ResolvConf {
  private final List<InetSocketAddress> nameservers;
  private final List<String> searchList;

  /**
   * Reads from the given reader and extracts the {@code nameserver}s and {@code search} domains
   * using the syntax of the {@code /etc/resolv.conf} file, see {@code man resolv.conf}.
   *
   * @param reader contents of {@code resolv.conf} are read from this {@link BufferedReader}, up to
   *     the caller to close it
   */
  static ResolvConf fromReader(BufferedReader reader) throws IOException {
    return new ResolvConf(reader);
  }

  /**
   * Reads the given file and extracts the {@code nameserver}s and {@code search} domains using the
   * syntax of the {@code /etc/resolv.conf} file, see {@code man resolv.conf}.
   */
  static ResolvConf fromFile(String file) throws IOException {
    try (var fileReader = new FileReader(file, UTF_8);
        BufferedReader reader = new BufferedReader(fileReader)) {
      return fromReader(reader);
    }
  }

  /**
   * Returns the {@code nameserver}s and {@code search} domains from the {@code /etc/resolv.conf}
   * file. The file is only read once during the lifetime of this class.
   */
  static ResolvConf system() {
    var resolvConv = ResolvConfLazy.machineResolvConf;
    if (resolvConv != null) {
      return resolvConv;
    }
    throw new IllegalStateException("/etc/resolv.conf could not be read");
  }

  private ResolvConf(BufferedReader reader) throws IOException {
    var nameservers = new ArrayList<InetSocketAddress>();
    var searchList = new ArrayList<String>();
    String ln;
    while ((ln = reader.readLine()) != null) {
      ln = ln.trim();
      if (ln.isEmpty()) {
        continue;
      }

      if (ln.startsWith("nameserver")) {
        ln = ln.substring("nameserver".length()).trim();
        nameservers.add(new InetSocketAddress(ln, 53));
      }
      if (ln.startsWith("search")) {
        ln = ln.substring("search".length()).trim();
        searchList.addAll(Arrays.asList(ln.split(" ")));
      }
    }
    this.nameservers = unmodifiableList(nameservers);
    this.searchList = unmodifiableList(searchList);
  }

  List<InetSocketAddress> getNameservers() {
    return nameservers;
  }

  List<String> getSearchList() {
    return searchList;
  }

  private static final class ResolvConfLazy {
    static final ResolvConf machineResolvConf;

    static {
      ResolvConf resolvConf;
      try {
        resolvConf = ResolvConf.fromFile("/etc/resolv.conf");
      } catch (IOException e) {
        resolvConf = null;
      }
      machineResolvConf = resolvConf;
    }
  }
}
