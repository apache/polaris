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
package org.apache.polaris.service.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.StorageControlClient;
import java.io.IOException;
import java.time.Instant;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.TableProperties;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcsStorageLocationPreparerTest {

  private static final String TEST_BUCKET = "my-test-bucket";
  private static final String TEST_TABLE_LOCATION =
      "gs://" + TEST_BUCKET + "/warehouse/ns1/table1";

  private Supplier<GoogleCredentials> credentialsSupplier;
  private Bucket mockBucket;
  private StorageControlClient mockControlClient;
  private GcsStorageLocationPreparer preparer;

  @BeforeEach
  void setUp() throws IOException {
    GoogleCredentials mockCredentials =
        GoogleCredentials.create(
            new AccessToken("test-token", Date.from(Instant.now().plusSeconds(3600))));
    credentialsSupplier = () -> mockCredentials;

    mockBucket = mock(Bucket.class);
    mockControlClient = mock(StorageControlClient.class);

    GcsStorageLocationPreparer realPreparer = new GcsStorageLocationPreparer(credentialsSupplier);
    preparer = spy(realPreparer);

    Mockito.doReturn(mockBucket).when(preparer).fetchBucketMetadata(anyString());
    Mockito.doReturn(mockControlClient).when(preparer).createStorageControlClient();
  }


  // ── buildTableHierarchy Tests (Internal Helper) ───────────────────────────

  @Nested
  class BuildTableHierarchyTests {

    @Test
    void producesCorrectHierarchyForTypicalTablePath() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("warehouse/ns1/table1");

      assertThat(folders)
          .containsExactly(
              "warehouse",
              "warehouse/ns1",
              "warehouse/ns1/table1",
              "warehouse/ns1/table1/metadata",
              "warehouse/ns1/table1/data");
    }

    @Test
    void handlesNestedNamespacePath() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("warehouse/ns1/ns2/ns3/table1");

      assertThat(folders)
          .containsExactly(
              "warehouse",
              "warehouse/ns1",
              "warehouse/ns1/ns2",
              "warehouse/ns1/ns2/ns3",
              "warehouse/ns1/ns2/ns3/table1",
              "warehouse/ns1/ns2/ns3/table1/metadata",
              "warehouse/ns1/ns2/ns3/table1/data");
    }

    @Test
    void handlesSingleSegmentPath() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("table1");

      assertThat(folders).containsExactly("table1", "table1/metadata", "table1/data");
    }

    @Test
    void stripsTrailingSlashes() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("warehouse/table1///");

      assertThat(folders)
          .containsExactly(
              "warehouse",
              "warehouse/table1",
              "warehouse/table1/metadata",
              "warehouse/table1/data");
    }

    @Test
    void alwaysIncludesMetadataAndDataLeafFolders() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("a/b/c");

      assertThat(folders)
          .filteredOn(f -> f.endsWith("/metadata") || f.endsWith("/data"))
          .containsExactly("a/b/c/metadata", "a/b/c/data");
    }

    @Test
    void collapsesRepeatedSlashesConsistently() {
      List<String> folders = GcsStorageLocationPreparer.buildTableHierarchy("warehouse//ns1///table1");

      assertThat(folders)
          .containsExactly(
              "warehouse",
              "warehouse/ns1",
              "warehouse/ns1/table1",
              "warehouse/ns1/table1/metadata",
              "warehouse/ns1/table1/data");
    }
  }

  // ── buildPathHierarchy Tests (Internal Helper) ─────────────────────────────

  @Nested
  class BuildPathHierarchyTests {

    @Test
    void producesFullHierarchyWithoutMetadataDataSubfolders() {
      List<String> folders =
          GcsStorageLocationPreparer.buildPathHierarchy("cdr/polaris-test-metadata/dsp/table1/metadata");

      assertThat(folders)
          .containsExactly(
              "cdr",
              "cdr/polaris-test-metadata",
              "cdr/polaris-test-metadata/dsp",
              "cdr/polaris-test-metadata/dsp/table1",
              "cdr/polaris-test-metadata/dsp/table1/metadata");
    }

    @Test
    void handlesSingleSegmentPath() {
      List<String> folders = GcsStorageLocationPreparer.buildPathHierarchy("data");

      assertThat(folders).containsExactly("data");
    }

    @Test
    void handlesTwoSegmentPath() {
      List<String> folders = GcsStorageLocationPreparer.buildPathHierarchy("custom/metadata");

      assertThat(folders).containsExactly("custom", "custom/metadata");
    }

    @Test
    void returnsEmptyForEmptyPath() {
      List<String> folders = GcsStorageLocationPreparer.buildPathHierarchy("");

      assertThat(folders).isEmpty();
    }

    @Test
    void collapsesRepeatedSlashes() {
      List<String> folders = GcsStorageLocationPreparer.buildPathHierarchy("a//b///c");

      assertThat(folders).containsExactly("a", "a/b", "a/b/c");
    }

    @Test
    void doesNotAppendMetadataOrDataSubfolders() {
      List<String> folders = GcsStorageLocationPreparer.buildPathHierarchy("warehouse/ns/table");

      assertThat(folders)
          .containsExactly("warehouse", "warehouse/ns", "warehouse/ns/table")
          .noneMatch(f -> f.endsWith("/metadata") && !f.equals("warehouse/ns/table"))
          .noneMatch(f -> f.endsWith("/data"));
    }
  }

  // ── prepareTableLocation: No-Op Scenarios ─────────────────────────────────

  @Nested
  class NoOpScenarioTests {

    @Test
    void skipsNullLocation() throws IOException {
      preparer.prepareTableLocation(null, Map.of());

      verify(preparer, never()).fetchBucketMetadata(anyString());
      verify(preparer, never()).createStorageControlClient();
    }

    @Test
    void skipsNonGcsLocation() throws IOException {
      preparer.prepareTableLocation("s3://my-bucket/warehouse/table1", Map.of());

      verify(preparer, never()).fetchBucketMetadata(anyString());
    }

    @Test
    void skipsEmptyLocation() throws IOException {
      preparer.prepareTableLocation("", Map.of());

      verify(preparer, never()).fetchBucketMetadata(anyString());
    }

    @Test
    void skipsBucketOnlyLocationWithNoObjectPath() throws IOException {
      preparer.prepareTableLocation("gs://" + TEST_BUCKET, Map.of());

      verify(preparer, never()).fetchBucketMetadata(anyString());
      verify(preparer, never()).createStorageControlClient();
    }

    @Test
    void skipsBucketWithRootSlashOnly() throws IOException {
      preparer.prepareTableLocation("gs://" + TEST_BUCKET + "/", Map.of());

      verify(preparer, never()).fetchBucketMetadata(anyString());
      verify(preparer, never()).createStorageControlClient();
    }
  }

  // ── prepareTableLocation: HNS Enabled ─────────────────────────────────────

  @Nested
  class HnsEnabledTests {

    @BeforeEach
    void setUpHnsBucket() {
      BucketInfo.HierarchicalNamespace hns = mock(BucketInfo.HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(true);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);
    }

    @Test
    void createsFolderHierarchyForHnsBucket() throws IOException {
      when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
          .thenReturn(Folder.getDefaultInstance());

      preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of());

      verify(preparer).createStorageControlClient();
      // warehouse, warehouse/ns1, warehouse/ns1/table1, metadata, data = 5 folders
      verify(mockControlClient, times(5)).createFolder(any(CreateFolderRequest.class));
    }

    @Test
    void gracefullyHandlesAlreadyExistingFolders() {
      AlreadyExistsException alreadyExists = createAlreadyExistsException();
      when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
          .thenThrow(alreadyExists);

      assertThatNoException().isThrownBy(() -> preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()));
    }

    @Test
    void mixedNewAndExistingFolders() {
      AlreadyExistsException alreadyExists = createAlreadyExistsException();

      when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
          .thenThrow(alreadyExists)
          .thenThrow(alreadyExists)
          .thenReturn(Folder.getDefaultInstance())
          .thenReturn(Folder.getDefaultInstance())
          .thenReturn(Folder.getDefaultInstance());

      assertThatNoException().isThrownBy(() -> preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()));
      verify(mockControlClient, times(5)).createFolder(any(CreateFolderRequest.class));
    }

    @Test
    void propagatesNonAlreadyExistsExceptionFromFolderCreation() {
      when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
          .thenThrow(new RuntimeException("gRPC transport failure"));

      assertThatThrownBy(() -> preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to create HNS folder");
    }
  }

  // ── prepareTableLocation: Flat Namespace ──────────────────────────────────

  @Nested
  class FlatNamespaceTests {

    @BeforeEach
    void setUpFlatBucket() {
      when(mockBucket.getHierarchicalNamespace()).thenReturn(null);
    }

    @Test
    void doesNotCreateFoldersForFlatNamespaceBucket() throws IOException {
      preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of());

      verify(preparer).fetchBucketMetadata(eq(TEST_BUCKET));
      verify(preparer, never()).createStorageControlClient();
    }

    @Test
    void doesNotCreateFoldersWhenHnsExplicitlyDisabled() throws IOException {
      BucketInfo.HierarchicalNamespace hns = mock(BucketInfo.HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(false);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);

      preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of());

      verify(preparer, never()).createStorageControlClient();
    }

    @Test
    void doesNotCreateFoldersWhenHnsEnabledIsNull() throws IOException {
      BucketInfo.HierarchicalNamespace hns = mock(BucketInfo.HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(null);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);

      preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of());

      verify(preparer, never()).createStorageControlClient();
    }
  }

  // ── HNS Cache Behavior ────────────────────────────────────────────────────

  @Nested
  class HnsCacheTests {

    @Test
    void queriesDifferentBucketsSeparately() {
      when(mockBucket.getHierarchicalNamespace()).thenReturn(null);

      preparer.prepareTableLocation("gs://bucket-a/warehouse/table1", Map.of());
      preparer.prepareTableLocation("gs://bucket-b/warehouse/table2", Map.of());

      verify(preparer).fetchBucketMetadata(eq("bucket-a"));
      verify(preparer).fetchBucketMetadata(eq("bucket-b"));
    }
  }

  // ── Error Handling ────────────────────────────────────────────────────────

  @Nested
  class ErrorHandlingTests {

    @Test
    void wrapsHnsCheckFailureWithDescriptiveMessage() {
      Mockito.doThrow(new RuntimeException("Network unreachable"))
          .when(preparer)
          .fetchBucketMetadata(anyString());

      assertThatThrownBy(() -> preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to check HNS status")
          .hasMessageContaining(TEST_BUCKET);
    }

    @Test
    void wrapsStorageControlClientInitFailure() throws IOException {
      BucketInfo.HierarchicalNamespace hns = mock(BucketInfo.HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(true);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);

      Mockito.doThrow(new IOException("gRPC init failure"))
          .when(preparer)
          .createStorageControlClient();

      assertThatThrownBy(() -> preparer.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to initialize StorageControlClient")
          .hasMessageContaining(TEST_BUCKET);
    }
  }

  // ── Cross-Bucket HNS Tests ────────────────────────────────────────────────

  @Nested
  class CrossBucketHnsTests {

    @Test
    void checksHnsForEachUniqueBucket() {
      // Table in bucket-a, metadata in bucket-b, data in bucket-c
      String tableLocation = "gs://bucket-a/warehouse/ns/table";
      Map<String, String> tableProperties =
          Map.of(
              TableProperties.WRITE_METADATA_LOCATION, "gs://bucket-b/custom/metadata",
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                  "gs://bucket-c/custom/data");

      // Mock HNS status: bucket-a=false, bucket-b=true, bucket-c=true
      Mockito.doReturn(createMockBucket("bucket-a", false))
          .when(preparer).fetchBucketMetadata("bucket-a");
      Mockito.doReturn(createMockBucket("bucket-b", true))
          .when(preparer).fetchBucketMetadata("bucket-b");
      Mockito.doReturn(createMockBucket("bucket-c", true))
          .when(preparer).fetchBucketMetadata("bucket-c");

      preparer.prepareTableLocation(tableLocation, tableProperties);

      // Should check HNS for all three buckets
      verify(preparer).fetchBucketMetadata("bucket-a");
      verify(preparer).fetchBucketMetadata("bucket-b");
      verify(preparer).fetchBucketMetadata("bucket-c");

      // Should only create folders in HNS-enabled buckets (bucket-b and bucket-c)
      // Full hierarchy for custom metadata path in bucket-b
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("bucket-b") && req.getFolderId().equals("custom")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("bucket-b") && req.getFolderId().equals("custom/metadata")));
      // Full hierarchy for custom data path in bucket-c
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("bucket-c") && req.getFolderId().equals("custom")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("bucket-c") && req.getFolderId().equals("custom/data")));
    }

    @Test
    void createsFoldersOnlyInHnsEnabledBuckets() {
      String tableLocation = "gs://hns-bucket/warehouse/ns/table";
      Map<String, String> tableProperties =
          Map.of(
              TableProperties.WRITE_METADATA_LOCATION, "gs://non-hns-bucket/metadata",
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                  "gs://hns-bucket/custom/data");

      // Mock HNS status: hns-bucket=true, non-hns-bucket=false
      Mockito.doReturn(createMockBucket("hns-bucket", true))
          .when(preparer).fetchBucketMetadata("hns-bucket");
      Mockito.doReturn(createMockBucket("non-hns-bucket", false))
          .when(preparer).fetchBucketMetadata("non-hns-bucket");

      preparer.prepareTableLocation(tableLocation, tableProperties);

      // Should create table hierarchy in hns-bucket
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("warehouse")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("warehouse/ns")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("warehouse/ns/table")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("warehouse/ns/table/metadata")));
      // Full hierarchy for custom data path
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("custom")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("hns-bucket") && req.getFolderId().equals("custom/data")));
    }

    @Test
    void handlesAllPathsInSameBucket() {
      String tableLocation = "gs://single-bucket/warehouse/ns/table";
      Map<String, String> tableProperties =
          Map.of(
              TableProperties.WRITE_METADATA_LOCATION, "gs://single-bucket/custom/metadata",
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                  "gs://single-bucket/custom/data");

      Mockito.doReturn(createMockBucket("single-bucket", true))
          .when(preparer).fetchBucketMetadata("single-bucket");

      preparer.prepareTableLocation(tableLocation, tableProperties);

      // Should check HNS only once for the single bucket
      verify(preparer, times(1)).fetchBucketMetadata("single-bucket");

      // Should create table hierarchy + full custom path hierarchies
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("warehouse")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("warehouse/ns")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("warehouse/ns/table")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("warehouse/ns/table/metadata")));
      // "custom" parent created for both metadata and data paths (AlreadyExists handled gracefully)
      verify(mockControlClient, times(2)).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("custom")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("custom/metadata")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("single-bucket") && req.getFolderId().equals("custom/data")));
    }

    @Test
    void skipsInvalidGcsLocations() {
      String tableLocation = "gs://valid-bucket/warehouse/ns/table";
      Map<String, String> tableProperties =
          Map.of(
              TableProperties.WRITE_METADATA_LOCATION, "s3://invalid-bucket/metadata", // Not GCS
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                  "gs://valid-bucket/data");

      Mockito.doReturn(createMockBucket("valid-bucket", true))
          .when(preparer).fetchBucketMetadata("valid-bucket");

      preparer.prepareTableLocation(tableLocation, tableProperties);

      // Should only check HNS for valid-bucket
      verify(preparer).fetchBucketMetadata("valid-bucket");
      verify(preparer, never()).fetchBucketMetadata("invalid-bucket");
      
      // Should create table hierarchy + data folder
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("valid-bucket") && req.getFolderId().equals("warehouse")));
      verify(mockControlClient).createFolder(argThat(req -> 
          req.getParent().contains("valid-bucket") && req.getFolderId().equals("data")));
    }

    @Test
    void handlesEmptyBucketNames() {
      String tableLocation = "gs:///invalid/path"; // Empty bucket name
      Map<String, String> tableProperties = Map.of();

      preparer.prepareTableLocation(tableLocation, tableProperties);

      // Should not attempt any storage operations
      verify(preparer, never()).fetchBucketMetadata(anyString());
      verify(mockControlClient, never()).createFolder(any());
    }

    private Bucket createMockBucket(String bucketName, boolean hnsEnabled) {
      Bucket bucket = mock(Bucket.class);
      when(bucket.getName()).thenReturn(bucketName);
      
      BucketInfo.HierarchicalNamespace hns = mock(BucketInfo.HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(hnsEnabled);
      when(bucket.getHierarchicalNamespace()).thenReturn(hns);
      
      return bucket;
    }
  }

  // ── StorageLocationPreparerFactory Tests ──────────────────────────────────

  @Nested
  class FactoryTests {

    @Test
    void noOpFactoryProducesNoOpPreparerForGcsConfig() {
      var factory =
          org.apache.polaris.service.storage.StorageLocationPreparerFactory.noOp();
      var gcsConfig = mock(org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo.class);

      var result = factory.create(gcsConfig);

      // Should not throw and should do nothing (no-op)
      Assertions.assertThatNoException()
          .isThrownBy(() -> result.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()));
    }

    @Test
    void noOpFactoryProducesNoOpPreparerForNonGcsConfig() {
      var factory =
          org.apache.polaris.service.storage.StorageLocationPreparerFactory.noOp();
      var awsConfig =
          mock(org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.class);

      var result = factory.create(awsConfig);

      Assertions.assertThatNoException()
          .isThrownBy(() -> result.prepareTableLocation(TEST_TABLE_LOCATION, Map.of()));
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static AlreadyExistsException createAlreadyExistsException() {
    StatusCode statusCode = mock(StatusCode.class);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);
    return new AlreadyExistsException(
        new RuntimeException("Folder already exists"), statusCode, false);
  }
}
