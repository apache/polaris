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

/**
 * Maintenance service implementation: do not directly use the types in this package.
 *
 * <p>Uses bloom filters to "collect" the references and objects to retain. The sizing of both
 * filters uses the values of scanned references/objects of the last <em>successful</em> maintenance
 * run, plus 10%. If no successful maintenance service run is present, the values of the maintenance
 * configuration will be used.
 */
package org.apache.polaris.persistence.nosql.maintenance.impl;
