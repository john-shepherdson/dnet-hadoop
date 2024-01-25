# This script deploys and runs the oozie workflow on the cluster, defined in the "~/.dhp/application.properties" file.

# Select the build profile.
DEFAULT_PROFILE=''  # It's the empty profile.
NEWER_VERSIONS_PROFILE='-Pscala-2.12'
CHOSEN_MAVEN_PROFILE=${DEFAULT_PROFILE}

# Build and deploy this module.
mvn clean package -U ${CHOSEN_MAVEN_PROFILE} -Poozie-package,deploy,run \
      -Dworkflow.source.dir=eu/dnetlib/dhp/continuous_validator

# Show the Oozie-job-ID.
echo -e "\n\nShowing the contents of \"extract-and-run-on-remote-host.log\":\n"
cat ./target/extract-and-run-on-remote-host.log
