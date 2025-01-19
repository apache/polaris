# Apache Polaris Release Infrastructure

This directory holds all the infrastructure parts that are required to draft a Polaris release (RC) and to eventually
release it.

Releases can be created from any Git commit, version branches are supported.

Generally, releases at the ASF follow the following workflow:

1. Draft a release
2. Start a VOTE on the dev mailing list
3. If the VOTE fails, the release has failed - "go to step 1"
4. If the VOTE passes, publish the release

The Polaris project decided to run all release related via GitHub actions - a release is never drafted nor actually
released from a developer machine.

## Technical process

In the `release/bin/` directory are the scripts that are required to draft a release and to publish it as "GA". All
scripts can be called with the `--help` argument to get some usage information. There is also a `--dry-run` option
to inspect what _would_ happen.

The technical process for a release follows the workflow above:

1. `releases/bin/draft-release.sh --major <major-version-number> --minor <minor-version-number> --commit <Git-commit-ID>`
   creates a release-candidate. The `--commit` argument is optional and defaults to the HEAD of the local Git
   worktree. The patch version number and RC-number are generated automatically. This means, that RC-numbers are
   automatically incremented as long as there is no "final" release tag. In that case, the patch version number
   is incremented and the RC-number is set to 1.

   The Git tag name that will be used follows the pattern `polaris-<major>.<minor>.<patch>-RC<rc-number>`

   The content of the `version.txt` file is set to the full release version, for example `1.2.3`.

   Gradle will run with the arguments `-Prelease publishToApache closeApacheStagingRepository sourceTarball`.
   Both the release artifacts (jars) and the source tarball are signed. The signing and Nexus credentials must be
   provided externally using the `ORG_GRADLE_PROJECT_*` environment variables. In dry-run mode, this step runs
   Gradle with the arguments `-PjarWithGitInfo -PsignArtifacts -PuseGpgAgent publishToMavenLocal sourceTarball publishToMavenLocal`.

   The staging repository ID, which is needed to release the staging repository, and URL are extracted from the
   Gradle output and memoized in the files `releases/current-release-staging-repository-id` and
   `releases/current-release-staging-repository-url`. The source Git commit ID is memoized in the file
   `releases/current-release-commit-id`.

   The source tarball will be uploaded to the Apache infrastructure.

   Release notes will be generated and included in the file `site/content/in-dev/unreleasd/release-notes.md`. This
   makes the release notes available later on the project website within the versioned docs.

   The suggested release VOTE email subject and body with the correct links and information are provided.

   The last step is to push the Git tag with a Git commit containing the above file changes. 
2. Once the release VOTE passed: `releases/bin/publish-release.sh --major <major-version-number> --minor <minor-version-number>`.

   The command will find the latest RC tag for the latest patch release of the given major/minor version.

   The final Git tag name that will be used follows the pattern `polaris-<major>.<minor>.<patch>` and created from
   the latest RC-tag for that version.

   The Git tag is then pushed.

   Documentation pages for the release will then be copied from the `site/content/in-dev/unreleased` into the
   `site/content/releases/<major>.<minor>.<patch>` folder within the `versioned-docs` branch.
   The changes for the updated `versioned-docs` branch are then pushed to Git.

   The suggested ANNOUNCEMENT email subject and body are provided.

   Creating a release in GitHub is the last step.

## Technical requirements

To use the scripts in the `releases/bin/` folder, it is required to have a _full_ clone with all tags and branches.
Shallow clones, which is the default when checking out a Git repository in GitHub actions, will _not_ work properly!

On top, all scripts need privileges to be able to push tags and branches to the GitHub repository - this is an
essential requirement for the scripts to do their job.

The `draft-release.sh` and `publish-release.sh` scripts also require the following environment variables providing
the necessary secrets:

* `ORG_GRADLE_PROJECT_signingKey`
* `ORG_GRADLE_PROJECT_signingPassword`
* `ORG_GRADLE_PROJECT_sonatypeUsername`
* `ORG_GRADLE_PROJECT_sonatypePassword`

GitHub actions running the scripts must provide those secrets and privileges.

## Version branches

The Polaris project may use maintenance version branches following the pattern `release/<major>.x` and
`release/<major>.<minor>`. The scripts mentioned above are already support version branches and have validations for
this use case. Using version branches is not a requirement for the release scripts.

The two scripts `releases/bin/create-release-branch.sh` plus the informative `releases/bin/list-release-branches.sh`
are there to help with version branches. The former must be invoked on the main branch and creates a new major-version
branch using the `release/<major>.x` pattern. The latter must be invoked on a major-version branch and creates a new
minor version branch using the `release/<major>.<minor>` pattern.

# TODO

* Close staging repo when RC is abandoned
* Adopt website (top bar menu, maintained releases, left site menu)
