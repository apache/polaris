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

package org.apache.polaris.test.rustfs;

import org.apache.polaris.testcontainer.S3Access;

/**
 * Provides access to Rustfs via a preconfigured S3 client and providing the by default randomized
 * bucket and access/secret keys.
 *
 * <p>Annotate JUnit test instance or static fields or method parameters of this type with {@link
 * Rustfs}.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.104.2
public interface RustfsAccess extends S3Access {}
