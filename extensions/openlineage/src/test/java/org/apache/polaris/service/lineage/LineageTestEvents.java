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
package org.apache.polaris.service.lineage;

import io.openlineage.server.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;

/** Builders for the three OpenLineage event shapes, so tests can stay about behaviour. */
final class LineageTestEvents {

  static final URI PRODUCER = URI.create("https://example.test/producer");
  static final URI SCHEMA_URL = URI.create("https://openlineage.io/spec/1-0-5/OpenLineage.json");
  static final ZonedDateTime EVENT_TIME = ZonedDateTime.parse("2026-09-19T12:00:00Z");

  private LineageTestEvents() {}

  static OpenLineage.InputDataset input(String namespace, String name) {
    return new OpenLineage.InputDataset(namespace, name, null, null);
  }

  static OpenLineage.OutputDataset output(String namespace, String name) {
    return new OpenLineage.OutputDataset(namespace, name, null, null);
  }

  static OpenLineage.StaticDataset staticDataset(String namespace, String name) {
    return new OpenLineage.StaticDataset(namespace, name, null);
  }

  static OpenLineage.RunEvent runEvent(
      List<OpenLineage.InputDataset> inputs, List<OpenLineage.OutputDataset> outputs) {
    return new OpenLineage.RunEvent(
        EVENT_TIME,
        PRODUCER,
        SCHEMA_URL,
        OpenLineage.RunEvent.EventType.COMPLETE,
        null,
        null,
        inputs,
        outputs);
  }

  static OpenLineage.JobEvent jobEvent(
      List<OpenLineage.InputDataset> inputs, List<OpenLineage.OutputDataset> outputs) {
    return new OpenLineage.JobEvent(EVENT_TIME, PRODUCER, SCHEMA_URL, null, inputs, outputs);
  }

  static OpenLineage.DatasetEvent datasetEvent(OpenLineage.StaticDataset dataset) {
    return new OpenLineage.DatasetEvent(EVENT_TIME, PRODUCER, SCHEMA_URL, dataset);
  }

  /** An event that references no datasets at all, e.g. a job-lifecycle START. */
  static OpenLineage.RunEvent datasetFreeEvent() {
    return runEvent(List.of(), List.of());
  }
}
