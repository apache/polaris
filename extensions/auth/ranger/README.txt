#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

Prerequisites
=================

1. Ranger authorizer for Polaris requires Apache Ranger version 2.8.0 or later.

Setup instructions
=================

1. Enable Ranger authorizer by setting the following property in
   application.properties:
#---------------------------------------------
polaris.authorization.type=ranger
#---------------------------------------------

2. Specify the location of Ranger authorizer configuration file by setting the
   following property in application.properties:
#---------------------------------------------
polaris.authorization.ranger.config-file-name=ranger-plugin.properties
#---------------------------------------------

  Ranger authorizer searches for the specified configuration file in classpath.
  It is recommended to place the configuration file in the same location as
  application.properties file.

  Consider copying sample file from sample-conf/ranger-plugin.properties to the
  folder where application.properties file is located and update the config
  file according your Ranger installation.

3. Restart Polaris to see that all accesses are authorized by Ranger policies,
   with access audit records available in Apache Ranger console.
