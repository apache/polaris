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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BUCKET_ROOT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createBucketRootCatalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createNamespaceAt;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createTableAt;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.statusOf;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.unstructuredServices;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import jakarta.ws.rs.core.Response;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;

/**
 * Vend-time refusal of table locations that carry an empty path segment. The module refuses such a
 * location at create and update (allowed locations) and at vend time (grant locations). The
 * create-time rule sees only the allowed locations, so a table create can still name a data path
 * with an empty path segment. A table create loads the server's own FileIO with vended credentials,
 * so that first vend refuses the data path with HTTP 400.
 */
class R2LocalSigningScopeFlowTest {

  private static String refusal(String location) {
    return "location '"
        + location
        + "' contains an empty path segment ('//'), which the R2_LOCAL_SIGNING credential vending"
        + " mechanism does not accept";
  }

  /** A {@code write.data.path} with an empty path segment is refused when the table is created. */
  @Test
  void aDataPathWithAnEmptyPathSegmentIsRefusedAtVendTime() {
    TestServices svc = unstructuredServices();
    String cat = "r2emptysegment";
    createBucketRootCatalog(svc, cat);
    createNamespaceAt(svc, cat, "ns", BUCKET_ROOT + "/ns");
    String dataPath = BUCKET_ROOT + "//ns/other/";

    Throwable thrown =
        catchThrowable(
            () -> createTableAt(svc, cat, "ns", "t1", BUCKET_ROOT + "/ns/t1", dataPath).close());

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessage(refusal(dataPath));
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  /** A {@code write.data.path} that is only an empty path segment after the bucket is refused. */
  @Test
  void aDataPathThatIsOnlyAnEmptyPathSegmentIsRefusedAtVendTime() {
    TestServices svc = unstructuredServices();
    String cat = "r2rootsegment";
    createBucketRootCatalog(svc, cat);
    createNamespaceAt(svc, cat, "ns", BUCKET_ROOT + "/ns");
    String dataPath = BUCKET_ROOT + "//";

    Throwable thrown =
        catchThrowable(
            () -> createTableAt(svc, cat, "ns", "t1", BUCKET_ROOT + "/ns/t1", dataPath).close());

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessage(refusal(dataPath));
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }
}
