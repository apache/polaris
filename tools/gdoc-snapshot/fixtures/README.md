<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Legacy snapshot fixtures

These small synthetic fixtures exercise migration from the earlier `source.json`
format. `pinned` records Google revision `12`; `current` deliberately has no Google
revision and must be rejected by migration. Neither fixture contains real Google
content. Their file bytes and JSON manifests are excluded from the header audit
because the tests verify preservation of the recorded hashes.
