### Fixes

- `bootstrap --credentials-file` no longer reports success for a file that names no realms. Such
  a file bootstrapped nothing and still exited 0; the command now names the file and exits with
  an error.
