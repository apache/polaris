### Fixes

- The cleanup tasks scheduled by a drop with purge now vend STS credentials with the dropped table's
  or view's catalog, namespace and table in the session tags and role session name
  (`SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL`, `SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL`).
  Previously the task's name was used as the catalog and table, and the namespace was missing.

### Upgrade notes

- Cleanup tasks scheduled by a drop with purge before upgrading don't carry the catalog name, so
  their STS credentials use `unknown` as the catalog session tag and leave the catalog out of the
  role session name.
