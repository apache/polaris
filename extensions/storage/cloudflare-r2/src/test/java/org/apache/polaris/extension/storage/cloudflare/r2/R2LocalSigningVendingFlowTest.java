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

import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.ACCESS_KEY;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.ACCOUNT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BASE;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BUCKET;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.BUCKET_ROOT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.CREDENTIAL_KEYS;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.ENDPOINT;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.FIXED_TOKEN;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.NOW;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.SCHEMA;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.SECRET_KEY;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.TOKEN;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.catalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createCatalog;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.createStructuredTable;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.decode;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.loadTable;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.loadTableResponse;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.prefixPaths;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.r2Config;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.rawJwt;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.recordingFileIOFactory;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.services;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.sha256Hex;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2FlowTestSupport.statusOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.LocalIcebergCatalog;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.junit.jupiter.api.Test;

/**
 * Credential vending through the resource layer for an S3 catalog whose {@code
 * credentialVendingMechanism} is {@code R2_LOCAL_SIGNING}: the delegated load contract, the
 * non-delegated load, and the cleanup-task FileIO.
 */
class R2LocalSigningVendingFlowTest {

  private static final String PREFIX_CLAIM_BASE = "base/";

  @Test
  void aDelegatedLoadCarriesTheVendingContract() throws Exception {
    TestServices svc = services(Map.of(), FIXED_TOKEN);
    String cat = "r2vend";
    String location = createStructuredTable(svc, cat, r2Config(List.of(BASE + "/" + cat)).build());

    LoadTableResponse loaded = loadTable(svc, cat, "ns", "t1", "vended-credentials");

    Map<String, String> config = loaded.config();
    assertThat(config.keySet()).containsAll(CREDENTIAL_KEYS);
    assertThat(config.get("s3.access-key-id")).isEqualTo(ACCESS_KEY);
    assertThat(config.get("s3.endpoint")).isEqualTo(ENDPOINT);
    assertThat(config.get("s3.path-style-access")).isEqualTo("true");
    assertThat(config.get("client.region")).isEqualTo("auto");

    // One storage-credentials entry, and its prefix is the table location.
    assertThat(loaded.credentials().size()).isEqualTo(1);
    Credential entry = loaded.credentials().get(0);
    assertThat(entry.prefix()).isEqualTo(location);
    assertThat(entry.config().keySet()).containsAll(CREDENTIAL_KEYS);

    String secret = config.get("s3.secret-access-key");
    String sessionToken = config.get("s3.session-token");
    assertThat(secret.equals(SECRET_KEY))
        .describedAs("the temporary secret is not the parent secret")
        .isFalse();
    assertThat(secret.equals(sha256Hex(rawJwt(sessionToken))))
        .describedAs("the temporary secret is the SHA-256 hex of the raw JWT")
        .isTrue();

    // The advertised expiry is NOW plus the default 3600 s lifetime, and the exp claim agrees.
    long expiresAtMs = Long.parseLong(config.get("s3.session-token-expires-at-ms"));
    assertThat(expiresAtMs).isEqualTo(NOW.plusSeconds(3600).toEpochMilli());

    DecodedJWT jwt = decode(sessionToken);
    assertThat(jwt.getAudience()).containsExactly(ACCOUNT + ".r2.cloudflarestorage.com");
    assertThat(jwt.getIssuer()).isEqualTo(ACCESS_KEY);
    assertThat(jwt.getSubject()).isEqualTo(ACCOUNT);
    assertThat(jwt.getClaim("bucket").asString()).isEqualTo(BUCKET);
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-write");
    assertThat(jwt.getExpiresAtAsInstant()).isEqualTo(NOW.plusSeconds(3600));
    assertThat(prefixPaths(jwt)).containsExactly(PREFIX_CLAIM_BASE + cat + "/ns/t1/");
  }

  @Test
  void aNonDelegatedLoadWorksWithATokenAndFailsWithTheVendTimeMessageWithoutOne() {
    AtomicReference<Optional<CloudflareR2ParentToken>> token =
        new AtomicReference<>(Optional.of(TOKEN));
    TestServices svc = services(Map.of(), storageName -> token.get());
    String cat = "r2plain";
    createStructuredTable(svc, cat, r2Config(List.of(BASE + "/" + cat)).build());

    // With a token: the load works, carries the S3 properties, and carries no credentials.
    LoadTableResponse loaded = loadTable(svc, cat, "ns", "t1", null);
    assertThat(loaded.config().keySet()).doesNotContainAnyElementsOf(CREDENTIAL_KEYS);
    assertThat(loaded.config().get("s3.endpoint")).isEqualTo(ENDPOINT);
    assertThat(loaded.config().get("s3.path-style-access")).isEqualTo("true");
    assertThat(loaded.config().get("client.region")).isEqualTo("auto");

    // The server loses its token (a redeploy without it): the same load now fails closed with the
    // vend-time message, which names no server property and no value.
    token.set(Optional.empty());
    Throwable thrown = catchThrowable(() -> loadTableResponse(svc, cat, "ns", "t1", null).close());

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No Cloudflare R2 parent token is configured on the server");
    assertThat(thrown.getMessage()).doesNotContain("polaris.storage");
    assertThat(statusOf(thrown)).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  /**
   * A cleanup (purge) task rebuilds a FileIO through {@code TaskFileIOSupplier}, which asks for the
   * {@code ALL} action on the table location, so the credential is read-write.
   */
  @Test
  void aCleanupTaskFileIoGetsAReadWriteCredential() {
    List<StorageAccessConfig> loadedConfigs = new CopyOnWriteArrayList<>();
    TestServices svc =
        services(
            Map.of(
                "SUPPORTED_CATALOG_STORAGE_TYPES", List.of("FILE", "S3"),
                "ALLOW_SPECIFYING_FILE_IO_IMPL", true,
                "ALLOW_INSECURE_STORAGE_TYPES", true,
                "DROP_WITH_PURGE_ENABLED", true),
            FIXED_TOKEN,
            recordingFileIOFactory(loadedConfigs));
    String cat = "r2purge";
    createCatalog(svc, catalog(cat, r2Config(List.of(BASE + "/" + cat)).build()));

    CallContext callContext = svc.newCallContext();
    LocalIcebergCatalog icebergCatalog =
        new LocalIcebergCatalog(
            svc.polarisDiagnostics(),
            svc.resolverFactory(),
            svc.metaStoreManager(),
            callContext,
            new PolarisPassthroughResolutionView(
                svc.resolutionManifestFactory(), svc.principal(), cat),
            svc.principal(),
            svc.taskExecutor(),
            svc.storageAccessConfigProvider(),
            svc.fileIOFactory(),
            svc.polarisEventDispatcher(),
            svc.eventMetadataFactory());
    icebergCatalog.initialize(cat, Map.of("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO"));

    TableIdentifier table = TableIdentifier.of(Namespace.of("newdb"), "table");
    icebergCatalog.createNamespace(table.namespace());
    String location = icebergCatalog.createTable(table, SCHEMA).location();
    assertThat(location).startsWith(BASE + "/" + cat + "/newdb/");
    icebergCatalog.dropTable(table, true);

    List<PolarisBaseEntity> tasks =
        svc.metaStoreManager()
            .loadTasks(callContext.getPolarisCallContext(), "testExecutor", PageToken.fromLimit(1))
            .getEntities();
    assertThat(tasks.size()).isEqualTo(1);
    TaskEntity task = TaskEntity.of(tasks.get(0));

    int before = loadedConfigs.size();
    FileIO fileIO =
        new TaskFileIOSupplier(svc.fileIOFactory(), svc.storageAccessConfigProvider())
            .apply(task, table);

    assertThat(fileIO).isNotNull();
    assertThat(loadedConfigs.size()).isEqualTo(before + 1);
    StorageAccessConfig taskConfig = loadedConfigs.get(before);
    assertThat(taskConfig.credentials().keySet()).containsAll(CREDENTIAL_KEYS);
    DecodedJWT jwt = decode(taskConfig.credentials().get("s3.session-token"));
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-write");
    assertThat(jwt.getClaim("bucket").asString()).isEqualTo(BUCKET);
    String path = location.substring((BUCKET_ROOT + "/").length());
    String expectedPrefix = path.endsWith("/") ? path : path + "/";
    assertThat(prefixPaths(jwt)).containsExactly(expectedPrefix);
  }

  /**
   * The Python CLI sends {@code stsUnavailable=false} and {@code kmsUnavailable=false}. A catalog
   * created that way must still vend: only {@code true} would push the access delegation resolver
   * to remote signing.
   */
  @Test
  void aCatalogWithTheCliDefaultFlagsStillVendsCredentials() {
    TestServices svc = services(Map.of(), FIXED_TOKEN);
    String cat = "r2cli";
    createStructuredTable(
        svc,
        cat,
        r2Config(List.of(BASE + "/" + cat))
            .setStsUnavailable(false)
            .setKmsUnavailable(false)
            .build());

    LoadTableResponse loaded = loadTable(svc, cat, "ns", "t1", "vended-credentials");

    assertThat(loaded.config().keySet()).containsAll(CREDENTIAL_KEYS);
    assertThat(loaded.credentials().size()).isEqualTo(1);
  }
}
