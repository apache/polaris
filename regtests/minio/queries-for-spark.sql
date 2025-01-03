--   Licensed to the Apache Software Foundation (ASF) under one
--   or more contributor license agreements.  See the NOTICE file
--   distributed with this work for additional information
--   regarding copyright ownership.  The ASF licenses this file
--   to you under the Apache License, Version 2.0 (the
--   "License"); you may not use this file except in compliance
--   with the License.  You may obtain a copy of the License at
 
--    http://www.apache.org/licenses/LICENSE-2.0
 
--   Unless required by applicable law or agreed to in writing,
--   software distributed under the License is distributed on an
--   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
--   KIND, either express or implied.  See the License for the
--   specific language governing permissions and limitations
--   under the License.

CREATE DATABASE IF NOT EXISTS db1;
CREATE DATABASE IF NOT EXISTS db1.ns1;
CREATE DATABASE IF NOT EXISTS db1.ns2;
CREATE OR REPLACE TABLE db1.ns1.table1 ( f1 int, f2 int );
INSERT INTO db1.ns1.table1 VALUES (10, 20);
INSERT INTO db1.ns1.table1 VALUES (11, 21);
INSERT INTO db1.ns1.table1 VALUES (12, 22);
SELECT * FROM db1.ns1.table1;

CREATE OR REPLACE VIEW db1.ns2.view1 ( line_count COMMENT 'Count of lines') AS SELECT COUNT(1) as qty FROM db1.ns1.table1;
SELECT * FROM db1.ns2.view1;
INSERT INTO db1.ns1.table1 VALUES (13, 23);
SELECT * FROM db1.ns2.view1;

CREATE DATABASE IF NOT EXISTS db1;
CREATE OR REPLACE TABLE db1.table1 ( f1 int, f2 int );
INSERT INTO db1.ns1.table1 VALUES (3, 2);

-- Test the second bucket allowed in the catalog
CREATE DATABASE IF NOT EXISTS db2 LOCATION 's3://warehouse2/polaris/';
CREATE OR REPLACE TABLE db2.table1 ( f1 int, f2 int );
INSERT INTO db2.table1 VALUES (01, 02);
SELECT * FROM db2.table1;

quit;
