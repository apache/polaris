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

package org.apache.polaris.core.connection;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import org.apache.polaris.core.admin.model.GcpAuthenticationParameters;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcpAuthenticationParametersDpoTest {

  public static final String QUOTA_PROJECT = "quotaProject";
  public static final String REMOTE_WAREHOUSE = "remoteWarehouse";

  private GcpAuthenticationParametersDpo dpo;

  @BeforeEach
  void setUp() {
    dpo = new GcpAuthenticationParametersDpo(QUOTA_PROJECT, REMOTE_WAREHOUSE);
  }

  @Test
  void testSerializeAndDeserialize() throws Exception {
    var connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://biglake.googleapis.com/iceberg/v1/restcatalog", dpo, null, null);

    assertEquals(
        connectionConfig.toString(),
        ConnectionConfigInfoDpo.deserialize(connectionConfig.serialize()).toString());
  }

  @Test
  void testConversionToDTOCapturesAllFields() {
    GcpAuthenticationParameters authenticationParameters = dpo.asAuthenticationParametersModel();
    Set<String> dtoGetMethods =
        Arrays.stream(GcpAuthenticationParameters.class.getDeclaredMethods())
            .map(Method::getName)
            .filter(x -> x.startsWith("get"))
            .collect(toUnmodifiableSet());
    dtoGetMethods.stream()
        .forEach(
            x -> {
              try {
                var expected = GcpAuthenticationParametersDpo.class.getMethod(x, null).invoke(dpo);
                var actual =
                    GcpAuthenticationParameters.class
                        .getMethod(x, null)
                        .invoke(authenticationParameters);
                assertEquals(expected, actual);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }
}
