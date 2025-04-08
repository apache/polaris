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
package org.apache.polaris.spark.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class SparkIcebergIT extends SparkIntegrationBase {
  @Test
  public void testNamespaces() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    String[] l1NS = new String[] {"l1ns1", "l1ns2"};
    for (String ns : l1NS) {
      onSpark(String.format("CREATE NAMESPACE %s", ns));
    }
    namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(2L);
    // String[] l2NS = new String[] {"l2ns1"};
    // onSpark(String.format("CREATE NAMESPACE %s.%s", l1NS[0], "l2ns1"));
    // namespaceCount = onSpark("SHOW NAMESPACES").count();
    // assertThat(namespaceCount).isEqualTo(2L);

    // drop the namespace

  }

  @Test
  public void testCreatDropView() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    String viewName = "test_view";
    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark(String.format("CREATE VIEW %s AS SELECT 1 AS id", viewName));
    onSpark("SHOW VIEWS");
    onSpark(String.format("DROP VIEW %s", viewName));
  }
}
