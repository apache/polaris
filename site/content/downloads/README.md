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

# Downloads Page Structure

This directory contains the downloads page for Apache Polaris releases.

## Structurek

```
downloads/
├── _index.md           # Landing page with links to all versions
├── 1.3.0/
│   └── index.md       # Version 1.3.0 details
├── 1.2.0/
│   └── index.md       # Version 1.2.0 details
└── ...
```

## Adding a New Release

When adding a new release, follow these steps:

1. Create a new version directory: Create `[major].[minor].[patch]/index.md`
   - Use an existing version page as a template (e.g., `1.3.0/index.md`)
   - Update the title, weight, and release date
   - Add the downloads table with links to artifacts
   - Include release notes

2. Update the landing page (`_index.md`):
   - Add the new version to "Active Releases"

3. Update the previous release's page:
   - Change all artifact URLs from `https://dlcdn.apache.org/` and `https://downloads.apache.org/` 
     to `https://archive.apache.org/dist/`

### URL Guidelines

- Current release: Use `https://dlcdn.apache.org/` for source/binary artifacts and `https://downloads.apache.org/` for signatures/checksums
- Previous releases: Use `https://archive.apache.org/dist/` for all artifacts
- Maven artifacts: Always use `https://repo1.maven.org/maven2/`

## Front Matter

Each version page should have the following front matter:

```yaml
---
title: "[version]"
linkTitle: "[version]"  # This appears in the sidebar
weight: [number]  # Decrement by 10 from previous version
hide_summary: true
exclude_search: true
type: docs
menus:
  main:
    parent: releases
    weight: [number]  # Same as page weight for consistency
    identifier: downloads-[version]  # Unique identifier to avoid conflicts
                                     # Example: downloads-1.3.0, downloads-1.2.0, etc.
---
```

No manual configuration in `site/hugo.yaml` is needed for individual version pages.

### Weight Guidelines

- Use the same weight for both the page weight and menu weight for simplicity
- Decrement by 10 for each older version (e.g., 1000, 990, 980, 970, etc.)
- This ensures both the sidebar and dropdown display versions in descending order (newest first)
- The "Overview" landing page uses weight 1 to appear first in the dropdown
- The "Latest" shortcut uses weight 2 to appear second in the dropdown menu (it doesn't appear in the sidebar)

## See Also

- [Manual Release Guide](../community/release-guides/manual-release-guide.md)
- [Semi-Automated Release Guide](../community/release-guides/semi-automated-release-guide.md)

