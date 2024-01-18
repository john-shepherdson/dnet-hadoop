# This script deploys and runs the oozie workflow.

DEFAULT_PROFILE=''  # It's the empty profile.
NEWER_VERSIONS_PROFILE='-Pscala-2.12'
CHOSEN_MAVEN_PROFILE=${DEFAULT_PROFILE}


mvn clean package ${CHOSEN_MAVEN_PROFILE} -Poozie-package,deploy,run \
      -Dworkflow.source.dir=eu/dnetlib/dhp/continuous_validator

echo -e "\n\nShowing the contents of \"extract-and-run-on-remote-host.log\":\n\n"
cat ./target/extract-and-run-on-remote-host.log
