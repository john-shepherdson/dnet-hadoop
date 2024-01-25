# Continuous Validation

This module is responsible for deploying an **Oozie Workflow** (on the desired cluster), which executes a **Spark** action.<br>
This action takes the HDFS-path of a directory of parquet files containing metadata records, and applies the validation process on all of them, in parallel. Then it outputs the results, in json-format, in the given directory.<br>
The validation process is powered by the [**uoa-validator-engine2**](https://code-repo.d4science.org/MaDgIK/uoa-validator-engine2) software.<br>

### Install and run

Run the **./installProject.sh** script and then the **./runOozieWorkflow.sh** script.<br>

[...]