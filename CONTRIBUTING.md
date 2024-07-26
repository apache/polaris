<!--
 Copyright (C) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Contributing to Polaris 

You want to contribute to Polaris: thank you!
Any contribution (code, test cases, documentation, use cases, ...) is valuable.

This documentation will help you to start your contribution.

## Report bugs and feature requests

You can report an issue in Polaris [issue tracker](https://github.com/polaris-catalog/polaris-dev/issues).

When reporting a bug make sure you document the steps to reproduce the issue and provide all necessary information (Apache Iceberg version, Catalog capabilities enabled, ...).
When creating a feature request document your requirements first. Please, try to not directly describe the solution.

If you want to dive into development yourself then you can also browse for open issues or features that need to be implemented. Take ownership of an issue and try fix it. Before doing a bigger change, please describe the concept/design of what you plan to do. If unsure if the design is good or will be accepted, discuss it as issue comments.

## Provide changes in a Pull Request

The best way to provide changes is to fork Polaris repository on GitHub and provide a Pull Request with your changes. To make it easy to apply your changes please use the following conventions:

* Every Pull Request should have a matching GitHub Issue.
* Create a branch that will house your change:

```bash
git clone https://github.com/polaris/polaris-dev
cd polaris-dev
git fetch --all
git checkout -b my-branch origin/main
```

  Don't forget to periodically rebase your branch:

```bash
git pull --rebase
git push GitHubUser my-branch --force
```

* Pull Requests should be based on the `main` branch.
* Test that your changes works by adapting or adding tests. Verify the build passes (see `README.md` for build instructions).
* If your Pull Request has conflicts with the `main` branch, please rebase and fix the conflicts.

## License

When contributing to this project, you agree that your contributions use the Apache License version 2. Please ensure you have permission to do this if required by your employer.
