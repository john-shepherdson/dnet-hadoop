# Ranking Workflow for Openaire Publications

This project contains the files for running a paper ranking workflow on the openaire graph using apache oozie.
All scripts are written in python and the project setup follows the typical oozie workflow structure:

- a workflow.xml file containing the workflow specification
- a job.properties file specifying parameter values for the parameters used by the workflow
- a set of python scripts used by the workflow

**NOTE**: the workflow depends on the external library of ranking scripts called BiP! Ranker.
You can check out a specific tag/release of BIP! Ranker using maven, as described in the following section.

## Check out a specific tag/release of BIP-Ranker

* Edit the `scmVersion` of the maven-scm-plugin in the pom.xml to point to the tag/release version you want to check out.

* Then, use maven to perform the checkout:

```
mvn scm:checkout
```

* The code should be visible under `src/main/bip-ranker` folder.