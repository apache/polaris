---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: Cloud Storage Configuration
type: docs
weight: 300
---

Polaris supports integration with various cloud storage systems, including Amazon S3, Azure Blob Storage, and Google Cloud Storage (GCS). This document provides detailed instructions for configuring Polaris with these storage providers.

---

## General Setup

### Configuration Structure

Polaris requires basic configuration for the selected cloud storage provider. Below is a generic example:

```yaml
storage:
  type: "<provider>" # Replace with 's3', 'azure', or 'gcs'
  config:
    key: "<YOUR_KEY>"
    secret: "<YOUR_SECRET>"
    region: "<YOUR_REGION>"
    bucket: "<YOUR_BUCKET_NAME>"