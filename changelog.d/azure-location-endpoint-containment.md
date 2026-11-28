### Fixes

- Azure storage locations on different endpoints, such as `abfss://c@acct.dfs.core.windows.net/path`
  and `abfss://c@acct.blob.core.windows.net/path`, are no longer treated as parent and child of each
  other when checking location containment. Azure schemes (such as `abfs` and `abfss`) are still
  excluded from comparisons.
