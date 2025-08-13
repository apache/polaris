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

package org.apache.polaris.persistence.relational.spanner;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;
import org.apache.polaris.persistence.relational.spanner.util.SpannerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class GoogleCloudSpannerDatabaseClientLifecycleManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GoogleCloudSpannerDatabaseClientLifecycleManager.class);

  protected final GoogleCloudSpannerConfiguration spannerConfiguration;
  protected final Spanner spanner;
  protected final DatabaseId databaseId;

  public GoogleCloudSpannerDatabaseClientLifecycleManager(
      GoogleCloudSpannerConfiguration spannerConfiguration) {
    this.spannerConfiguration = spannerConfiguration;
    spanner = SpannerUtil.spannerFromConfiguration(spannerConfiguration);
    databaseId = SpannerUtil.databaseFromConfiguration(spannerConfiguration);
  }

  protected List<String> getSpannerDatabaseDdl(SchemaOptions options) {
    final InputStream schemaStream;
    if (options.schemaFile() != null) {
      try {
        schemaStream = new FileInputStream(options.schemaFile());
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load file " + options.schemaFile(), e);
      }
    } else {
      if (options.schemaVersion() == null || options.schemaVersion() == 1) {
        schemaStream =
            getClass().getResourceAsStream("/org/apache/polaris/persistence/spanner/schema-v1.sql");
      } else {
        throw new IllegalArgumentException("Unknown schema version " + options.schemaVersion());
      }
    }
    try (schemaStream) {
      String schema = new String(schemaStream.readAllBytes(), Charset.forName("UTF-8"));
      List<String> lines = new ArrayList<>();
      for (String s : schema.split("\n")) {
        s = s.trim();
        if (s.startsWith("--") || s.length() == 0) {
          continue;
        }
        lines.add(s);
      }
      return List.of(String.join(" ", lines).split(";"));
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve DDL statements", e);
    }
  }

  @Produces
  public SchemaInitializer getSchemaInitializer() {
    return (options) -> {
      List<String> ddlStatements = getSpannerDatabaseDdl(options);
      LOGGER.info(
          "Attempting to initialize Spanner database DDL with {} statements,",
          ddlStatements.size());
      DatabaseAdminClient client = spanner.getDatabaseAdminClient();
      Database dbInfo =
          client.newDatabaseBuilder(databaseId).setDialect(Dialect.GOOGLE_STANDARD_SQL).build();
      try {
        client.updateDatabaseDdl(dbInfo, ddlStatements, null).get();
        LOGGER.info("Successfully applied DDL update.");
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(
            "Unable to update Spanner DDL. Please disable this option for this database configuration.",
            e);
      }
    };
  }

  @Produces
  public DatabaseClientSupplier getDatabaseClientSupplier() {
    return () -> spanner.getDatabaseClient(databaseId);
  }

  @Produces
  public DatabaseAdminClientSupplier getDatabaseAdminClientSupplier() {
    return () -> spanner.getDatabaseAdminClient();
  }
}
