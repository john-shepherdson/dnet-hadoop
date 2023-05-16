# Ranking Workflow for OpenAIRE Publications

This project contains the files for running a paper ranking workflow on the openaire graph using apache oozie.
All scripts are written in python and the project setup follows the typical oozie workflow structure:

- a workflow.xml file containing the workflow specification
- a job.properties file specifying parameter values for the parameters used by the workflow
- a set of python scripts used by the workflow

**NOTE**: the workflow depends on the external library of ranking scripts called [BiP! Ranker](https://github.com/athenarc/Bip-Ranker).
You can check out a specific tag/release of BIP! Ranker using maven, as described in the following section.

## Build and deploy

Use the following command for packaging:

```
mvn package -Poozie-package -Dworkflow.source.dir=eu/dnetlib/dhp/oa/graph/impact_indicators -DskipTests
```

Deploy and run:
```
mvn package -Poozie-package,deploy,run -Dworkflow.source.dir=eu/dnetlib/dhp/oa/graph/impact_indicators -DskipTests
```

Note: edit the property `bip.ranker.tag` of the `pom.xml` file to specify the tag of [BIP-Ranker](https://github.com/athenarc/Bip-Ranker) that you want to use.


Job info and logs: 
```
export OOZIE_URL=http://iis-cdh5-test-m3:11000/oozie
oozie job -info <jobId>
oozie job -log <jobId>
```

where `jobId` is the id of the job returned by the `run_workflow.sh` script.