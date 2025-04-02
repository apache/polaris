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

-- Create two more databases for testing. The first database, polaris_realm1, is created
-- during container initialization. See PostgresTestResourceLifecycleManager.

-- Note: the database names must follow the pattern polaris_{realm}. That's the pattern
-- specified by the persistence.xml file used in tests.

CREATE DATABASE polaris_realm2;
GRANT ALL PRIVILEGES ON DATABASE polaris_realm2 TO polaris;

CREATE DATABASE polaris_realm3;
GRANT ALL PRIVILEGES ON DATABASE polaris_realm3 TO polaris;
