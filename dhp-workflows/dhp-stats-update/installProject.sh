# Install the whole "dnet-hadoop" project.

# Delete this module's previous build-files in order to avoid any conflicts.
rm -rf target/ ||

# Go to the root directory of this project.
cd ../../

# Select the build profile.
DEFAULT_PROFILE=''  # It's the empty profile.
NEWER_VERSIONS_PROFILE='-Pscala-2.12'
CHOSEN_MAVEN_PROFILE=${DEFAULT_PROFILE}

# Install the project.
mvn clean install -U ${CHOSEN_MAVEN_PROFILE} -Dmaven.test.skip=true

# We skip tests for all modules, since the take a big amount of time and some of them fail.
# Any test added to this module, will be executed in the "runOozieWorkflow.sh" script.
