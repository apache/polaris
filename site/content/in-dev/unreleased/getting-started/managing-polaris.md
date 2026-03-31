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
Title: Managing Polaris
type: docs
weight: 100
---

This guide describes how to use the `setup` command to manage Apache Polaris configuration using an infrastructure-as-code approach. Define your Polaris configuration in a single YAML file and apply it with a single command.

This command supports bootstrapping new environments and exporting existing configurations for reuse or version control.

## Exporting Your Configuration

If you already have an Apache Polaris environment, you can export its current state to a YAML file using the `export` subcommand:

```bash
polaris setup export > polaris_bootstrap.yaml
```

This generates a readable YAML file containing principals, principal roles, catalogs, and their associated namespaces and catalog roles.

## Applying a Configuration

Use the `apply` subcommand to bootstrap a new environment or extend an existing one. The command reads your YAML file and performs the necessary create and grant operations in the correct order.

### Example Configuration

The following example [`simple-setup-config.yaml`](https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/assets/polaris/simple-setup-config.yaml) demonstrates the structure of a setup configuration file. For a complete reference of all supported options, see [`reference-setup-config.yaml`](https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/assets/polaris/reference-setup-config.yaml).

```yaml
# ==================================
#        Global Entities
# ==================================
principals:
  quickstart_user:
    roles:
      - quickstart_user_role

principal_roles:
  - quickstart_user_role

# ==================================
#     Catalog-Specific Entities
# ==================================
catalogs:
  - name: "quickstart_catalog"
    storage_type: "file"
    default_base_location: "file:///var/tmp/quickstart_catalog/"
    allowed_locations:
      - "file:///var/tmp/quickstart_catalog/"
    roles:
      quickstart_catalog_role:
        assign_to:
          - quickstart_user_role
        privileges:
          catalog:
            - CATALOG_MANAGE_CONTENT
    namespaces:
      - dev_namespace
```

### Applying the Setup

Before making any changes, you can preview what will be executed using the `--dry-run` flag:

```bash
polaris setup apply --dry-run site/content/guides/assets/polaris/simple-setup-config.yaml
```

Once satisfied, run the command to apply the changes:

```bash
polaris setup apply site/content/guides/assets/polaris/simple-setup-config.yaml
```

## Known Limitations

The current implementation focuses on simplifying initial setup, with a few limitations to be aware of:

- **Non-declarative updates**: The command is create-only. If an entity already exists, it will be skipped rather than updated. There is no state reconciliation yet.
- **Policy attachment export**: Policy attachments are not included in `setup export` due to performance considerations. However, they can still be defined in YAML and applied during `setup apply`.
- **External catalog testing**: Support for external catalogs (e.g., Hive Metastore) exists, but full end-to-end testing has not yet been completed. It is recommended to validate configurations in a non-production environment first.


