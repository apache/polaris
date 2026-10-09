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
package org.apache.polaris.service.catalog.directory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLConnection;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.parquet.Parquet;
import org.apache.polaris.service.catalog.io.ExceptionMappingFileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists the objects under a directory base location and replaces the content of the directory
 * inventory table (see {@link DirectoryCatalogHandler#DIRECTORY_TABLE_SCHEMA}) with them, in a
 * single commit.
 */
final class DirectoryScanner {
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryScanner.class);

  private DirectoryScanner() {}

  /**
   * @param sourceIO the {@link FileIO} used to list the objects, it must support prefix operations
   * @return the number of objects recorded in the table
   */
  static long scan(
      Table table,
      FileIO sourceIO,
      String baseLocation,
      List<String> include,
      List<String> exclude) {
    // The FileIO factory wraps the FileIO, and the wrapper does not expose the prefix operations
    FileIO io = sourceIO instanceof ExceptionMappingFileIO w ? w.getInnerIo() : sourceIO;
    if (!(io instanceof SupportsPrefixOperations prefixIO)) {
      throw new UnsupportedOperationException(
          "Scanning is not supported for location " + baseLocation);
    }
    List<Pattern> includePatterns = compile(include);
    List<Pattern> excludePatterns = compile(exclude);

    List<Record> records = new ArrayList<>();
    for (FileInfo file : prefixIO.listPrefix(baseLocation)) {
      String uri = file.location();
      if (matches(uri, includePatterns, excludePatterns)) {
        Record record = GenericRecord.create(table.schema());
        record.setField("file_uri", uri);
        record.setField("content_type", URLConnection.guessContentTypeFromName(uri));
        record.setField("size", file.size());
        record.setField(
            "last_modified",
            OffsetDateTime.ofInstant(Instant.ofEpochMilli(file.createdAtMillis()), ZoneOffset.UTC));
        records.add(record);
      }
    }

    OverwriteFiles overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue());
    String dataFileLocation = null;
    try {
      if (!records.isEmpty()) {
        dataFileLocation = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
        overwrite.addFile(write(table, dataFileLocation, records));
      }
      overwrite.commit();
    } catch (RuntimeException e) {
      if (dataFileLocation != null) {
        deleteQuietly(table, dataFileLocation);
      }
      throw e;
    }
    LOGGER.debug("Scanned {} objects under {}", records.size(), baseLocation);
    return records.size();
  }

  private static DataFile write(Table table, String location, List<Record> records) {
    OutputFile output = table.io().newOutputFile(location);
    try (DataWriter<Record> writer =
        Parquet.writeData(output)
            .schema(table.schema())
            .withSpec(table.spec())
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {
      records.forEach(writer::write);
      writer.close();
      return writer.toDataFile();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write the directory inventory file", e);
    }
  }

  private static void deleteQuietly(Table table, String location) {
    try {
      table.io().deleteFile(location);
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to delete the orphan inventory file {}", location, e);
    }
  }

  private static List<Pattern> compile(List<String> regexes) {
    return regexes == null ? List.of() : regexes.stream().map(Pattern::compile).toList();
  }

  private static boolean matches(String uri, List<Pattern> include, List<Pattern> exclude) {
    return (include.isEmpty() || include.stream().anyMatch(p -> p.matcher(uri).find()))
        && exclude.stream().noneMatch(p -> p.matcher(uri).find());
  }
}
