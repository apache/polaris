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
package org.apache.polaris.test.hms;

/**
 * Access to a running Hive Metastore instance, exposed as a Thrift endpoint reachable from the test
 * JVM. Annotate JUnit 5 instance/static fields or method parameters of this type with {@link Hms}.
 */
public interface HmsAccess {

  /** Host and port at which the Hive Metastore is reachable, separated by '{@code :}'. */
  String hostPort();

  /** Thrift URI ({@code thrift://host:port}) for the Hive Metastore. */
  String thriftUri();
}
