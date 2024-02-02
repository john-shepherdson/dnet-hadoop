# Continuous Validation

This module is responsible for deploying an **Oozie Workflow** (on the desired cluster), which executes a **Spark** action.<br>
This action takes the HDFS-path of a directory of parquet files containing metadata records, and applies the validation process on all of them, in parallel. Then it outputs the results, in json-format, in the given directory.<br>
The validation process is powered by the "[**uoa-validator-engine2**](https://code-repo.d4science.org/MaDgIK/uoa-validator-engine2)" software,
which is included as a dependency inside the main "pom.xml" file.<br>


### Configure the workflow

Add the wanted values for each of the parameters, defined in the "/src/main/resources/eu/dnetlib/dhp/continuous_validator/oozie_app/workflow.xml" file.<br>
The most important parameters are the following:
- ***parquet_path***: the input parquet
- ***openaire_guidelines***: valid values: "4.0", "3.0", "2.0", "fair_data", "fair_literature_v4"
- ***output_path***: Be careful to use a base directory which is different from the one that this module is running on, as during a new deployment, that base directory will be deleted.


### Install the project and then deploy and run the workflow

Run the **./installProject.sh** script and then the **./runOozieWorkflow.sh** script.<br>

Use the "workflow-id" displayed by the "runOozieWorkflow.sh" script to check the running status and logs, in the remote machine, as follows:
- Check the status: `oozie job -oozie http://<cluster's domain and port>/oozie -info <Workflow-ID>`
- Copy the "Job-id" from the output of the above command (numbers with ONE underscore between them).
- Check the job's logs (not the app's logs!): `yarn logs -applicationId application_<Job-ID>`
<br><br>

**Note**:<br>
If you encounter any "java.lang.NoSuchFieldError" issues in the logs, rerun using the following steps:
- Delete some remote directories related to the workflow in your user's dir: /user/<userName>/
  - ***.sparkStaging***
  - ***oozie-oozi***
- Run the **./installProject.sh** script and then the **./runOozieWorkflow.sh** script.
