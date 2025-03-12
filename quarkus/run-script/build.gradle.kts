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

val runScript by
  configurations.creating { description = "Used to provide the run.sh script" }

description = "Provides run.sh script for Quarkus fast-jar for distribution tar/zip"

// This is a separate project that only provides run scripts (run.sh).
// It is a separate project, because it is not good practice to directly
// reference files outside any project.
//
// Artifacts are NOT published as a Maven artifacts.

artifacts {
  add(runScript.name, project.layout.projectDirectory.file("scripts/run.sh"))
}

// Need this task to be present, there are no checks/tests in this project though
tasks.register("check")
