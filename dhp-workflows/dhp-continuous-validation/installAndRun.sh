# This script installs and runs the project.

DEFAULT_PROFILE=''  # It's the empty profile.
NEWER_VERSIONS_PROFILE='-Pscala-2.12'
CHOSEN_MAVEN_PROFILE=${DEFAULT_PROFILE}


# For error-handling, we cannot use the "set -e" since: it has problems https://mywiki.wooledge.org/BashFAQ/105
# So we have our own function, for use when a single command fails.
handle_error () {
  echo -e "\n\n$1\n\n"; exit $2
}

# Change the working directory to the script's directory, when running from another location.
cd "${0%/*}" || handle_error "Could not change-dir to this script's dir!" 1

if [[ $# -eq 0 ]]; then
  	echo -e "Wrong number of arguments given: ${#}\nPlease execute it like: installAndRun.sh <sparkMaster> <justRun: 0 | 1>";	exit 2
fi

sparkMaster=""
justRun=0

if [[ $# -eq 1 ]]; then
  sparkMaster=$1
elif [[ $# -eq 2 ]]; then
  sparkMaster=$1
	justRun=$2
elif [[ $# -gt 2 ]]; then
	echo -e "Wrong number of arguments given: ${#}\nPlease execute it like: installAndRun.sh <sparkMaster> <justRun: 0 | 1>";	exit 3
fi

if [[ justRun -eq 0 ]]; then
  mvn clean install ${CHOSEN_MAVEN_PROFILE}
fi
ContinuousValidator
test_parquet_file="./src/test/resources/part-00589-733117df-3822-4fce-bded-17289cc5959a-c000.snappy.parquet"
java -jar ./target/dhp-continuous-validation-1.0.0-SNAPSHOT.jar ${sparkMaster} ${test_parquet_file} 1
