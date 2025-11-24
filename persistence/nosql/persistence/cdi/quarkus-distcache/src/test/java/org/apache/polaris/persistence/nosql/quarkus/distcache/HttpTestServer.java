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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/** HTTP test server. */
public class HttpTestServer implements AutoCloseable {
  private final HttpServer server;

  public HttpTestServer(String context, HttpHandler handler) throws IOException {
    this(new InetSocketAddress("localhost", 0), context, handler);
  }

  public HttpTestServer(InetSocketAddress bind, String context, HttpHandler handler)
      throws IOException {
    HttpHandler safeHandler =
        exchange -> {
          try {
            handler.handle(exchange);
          } catch (RuntimeException | Error e) {
            exchange.sendResponseHeaders(503, 0);
            throw e;
          }
        };
    server = HttpServer.create(bind, 0);
    server.createContext(context, safeHandler);
    server.setExecutor(null);

    server.start();
  }

  public InetSocketAddress getAddress() {
    return server.getAddress();
  }

  public URI getUri() {
    return URI.create(
        "http://"
            + getAddress().getAddress().getHostAddress()
            + ":"
            + getAddress().getPort()
            + "/");
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
