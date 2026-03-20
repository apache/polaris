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

package org.apache.polaris.core.connection.iceberg;

import static org.assertj.core.api.Assertions.assertThatCharSequence;

import java.util.Map;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.GcpAuthenticationParametersDpo;
import org.junit.jupiter.api.Test;

class IcebergRestConnectionConfigInfoDpoTest {

  @Test
  void testRoundTrip() {
    IcebergRestConnectionConfigInfoDpo dpo =
        new IcebergRestConnectionConfigInfoDpo(
            "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            new GcpAuthenticationParametersDpo(),
            null,
            null,
            Map.of("x", "y"));
    ConnectionConfigInfo dto = dpo.asConnectionConfigInfoModel(null);
    assertThatCharSequence(dpo.toString())
        .isEqualTo(
            ConnectionConfigInfoDpo.fromConnectionConfigInfoModelWithSecrets(dto, Map.of())
                .toString());
  }
}
