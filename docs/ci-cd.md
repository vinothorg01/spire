## Intro to Github Actions Workflows and CI/CD

Continuous Integration, Continuous Delivery to GHCR (github container repository), and Continuous Deployment to the Staging Astronomer Deployment, are managed via Github Actions Workflows (NOT to be confused with the Spire Workflows).  
  
While there are already other github actions workflows and more could be implemented in the future, this documentation focuses mostly on `ci.yml` and `cd.yml`, and broadly on how github actions workflows work and how to use them.

### Basics

How the workflows are triggered depends on the yml file, but in the case of a Pull Request, the workflows and specific jobs triggered can be seen in the checks for the PR from the github web UI, or from the Actions tab also on the github web UI.  
  
Additionally, in some cases such as with cd, after certain jobs complete, they will push slack notifications via a Spire slack bot to the private channel #data-spire-contributors.  
  
### Local-use / Debugging
The `act` CLI (https://github.com/nektos/act) is a useful tool for running github actions workflows locally, and is good for debugging because you can set it to skip certain jobs, pass environment variables freely, etc.  
  
Read the act documentation for more information, but below is an example use case.  
  
Example usage for running the `build-and-push-docker` job in `cd.yml`:  
  
```shell
$ pip install act
$ act -j build-and-push-docker -s ROBOT_GITHUB_USERNAME=<username> -s ROBOT_GITHUB_ACCESS_TOKEN=<token> -s CONDENAST_DATA_ROBOT_SSH_KEY="$(cat ~/.ssh/id_rsa)" -s ASTRO_STAGING_SERVICE_ACCOUNT=<token> -s SLACK_WEBHOOK=<url>
```  
  
## Continuous Integration (ci.yml)  
  
Consists of two jobs which run in parallel on github, `build-and-test-docker`, and `test-osx`. Both trigger automatically when a commit is pushed to a PR.  
  
`test-osx` installs python, submodules, requirements, and Spire itself onto a mac machine and runs linting, formatting, and the tests, excluding the integrations tests, which cannot be set up with docker and/or with github actions for reasons I (Max) no longer remember offhand. This would be the equivalent of setting up and running Spire locally on a mac.  
  
`build-and-test-docker` builds an ubuntu docker image of Spire and runs all of the tests, including the integrations tests.  
  
These checks must pass in order to merge a PR into main, barring admin override.  
  
## Continuous Delivery / Deployment (cd.yml)  
  
Consists of two jobs which run in parallel on github, `build-and-push-docker`, and `build-and-upload-wheel`. Both are triggered either when a release is published or a PR is pushed to main, but the behaviors in these two cases are different. Merging a PR to main will do full continuous deployment to the Staging Astronomer Deployment, whereas publishing a release will only do continuous delivery of the build as a docker image on GHCR.  
  
`build-and-push-docker` first creates a docker image of the Spire version and pushes it to ghcr. If it is a release, it will push to the Spire package, otherwise it pushes to the Spire-Dev package. These each can contain multiple images for different Spire versions, as can be seen in the packages tab on Spire in the github web UI. Additionally, on release, it will push both the Spire version image, and also overwrite the `latest` and `stable` Spire images.  
  
If non-release, there is an additional step where it will then build the spire-astronomer image and push the new Spire build to the Staging Astronomer Deployment automatically.  
  
`build-and-upload-wheel` creates a python wheel distribution of Spire and pushes it to dbfs (databricks file system) at `dbfs:/spire/packages/`. This is rarely used anymore, but still maintained. It will overwrite any current build with the same Spire version, and in the case of a release, will also overwrite the `latest` and `stable` builds.  
  