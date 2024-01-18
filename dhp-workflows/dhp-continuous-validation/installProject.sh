cd ../../

DEFAULT_PROFILE=''  # It's the empty profile.
NEWER_VERSIONS_PROFILE='-Pscala-2.12'
CHOSEN_MAVEN_PROFILE=${DEFAULT_PROFILE}

mvn clean install ${CHOSEN_MAVEN_PROFILE} -Dmaven.test.skip=true

# We skip tests for all modules, since the take a big amount of time and some of them fail.
# Any test added to this module, will be executed in the "runOozieWorkflow.sh" script.
