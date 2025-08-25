# Releasey Scripts

This directory contains scripts and libraries that are used exclusively by GitHub workflows to automate the Polaris release process.

## Important Notice

⚠️ **These scripts are designed for automation and should only be used by GitHub workflows.**

The scripts in this directory are:
- Optimized for the GitHub Actions environment
- Designed to work with specific workflow contexts and environment variables
- Not intended for manual execution by release managers

## Release Process Overview

The Polaris release automation follows a structured workflow as illustrated in the flowchart below. The process involves both automated GitHub workflows and manual steps performed by release managers.

![Release Process Flowchart](release-process-flowchart.png)

## GitHub Workflows

The release automation is implemented through the following GitHub workflows:

1. **[Create Release Branch](../.github/workflows/release-1-create-release-branch.yml)** - Creates a new release branch from a specified Git SHA
2. **[Update Release Candidate](../.github/workflows/release-2-update-release-candidate.yml)** - Updates version files, finalizes changelog, and creates RC tags
3. **[Build and Publish Artifacts](../.github/workflows/release-3-build-and-publish-artifacts.yml)** - Consolidated workflow that:
   - Performs prerequisite checks (tag validation, version extraction)
   - Builds source/binary artifacts and publishes to Nexus staging
   - Builds Docker images for server and admin tool
   - Builds Helm charts and stages them to dist dev repository
4. **[Publish Release](../.github/workflows/release-4-publish-release.yml)** - Finalizes the release:
   - Copies distribution from dist dev to dist release space
   - Creates a final release tag and GitHub release
   - Publishes Docker images to Docker Hub
   - Releases the candidate repository on Nexus

## Directory Structure

- `libs/` - Shared library functions used across release scripts
  - `_constants.sh` - Common constants and configuration
  - `_exec.sh` - Command execution utilities
  - `_github.sh` - GitHub API interaction functions
  - `_log.sh` - Logging utilities
  - `_version.sh` - Version handling functions