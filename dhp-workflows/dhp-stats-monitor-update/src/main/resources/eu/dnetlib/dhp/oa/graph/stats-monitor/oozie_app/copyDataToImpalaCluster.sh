export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export HADOOP_USER_NAME=$2

SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR=0


# Set the active HDFS node of OCEAN and IMPALA cluster.
OCEAN_HDFS_NODE='hdfs://nameservice1'
echo -e "\nOCEAN HDFS virtual-name which resolves automatically to the active-node: ${OCEAN_HDFS_NODE}"

IMPALA_HDFS_NODE=''
COUNTER=0
while [ $COUNTER -lt 3 ]; do
  if hdfs dfs -test -e hdfs://impala-cluster-mn1.openaire.eu/tmp >/dev/null 2>&1; then
      IMPALA_HDFS_NODE='hdfs://impala-cluster-mn1.openaire.eu:8020'
      break
  elif hdfs dfs -test -e hdfs://impala-cluster-mn2.openaire.eu/tmp >/dev/null 2>&1; then
      IMPALA_HDFS_NODE='hdfs://impala-cluster-mn2.openaire.eu:8020'
      break
  else
      IMPALA_HDFS_NODE=''
      sleep 1
  fi
  ((COUNTER++))
done
if [ -z "$IMPALA_HDFS_NODE" ]; then
    echo -e "\n\nERROR: PROBLEM WHEN SETTING THE HDFS-NODE FOR IMPALA CLUSTER! | AFTER ${COUNTER} RETRIES.\n\n"
    exit 1  # This is fatal and we have to exit independently of the "SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR" config, as none of the DBs will be able to get transferred.
fi
echo -e "Active IMPALA HDFS Node: ${IMPALA_HDFS_NODE} , after ${COUNTER} retries.\n\n"

IMPALA_HOSTNAME='impala-cluster-dn1.openaire.eu'
IMPALA_CONFIG_FILE='/etc/impala_cluster/hdfs-site.xml'

IMPALA_HDFS_DB_BASE_PATH="${IMPALA_HDFS_NODE}/user/hive/warehouse"

# Set sed arguments.
LOCATION_HDFS_NODE_SED_ARG="s|${OCEAN_HDFS_NODE}|${IMPALA_HDFS_NODE}|g" # This requires to be used with "sed -e" in order to have the "|" delimiter (as the "/" conflicts with the URIs)


function print_elapsed_time()
{
  start_time=$1
  end_time=$(date +%s)
  elapsed_time=$(($end_time-$start_time))
  hours=$((elapsed_time / 3600))
  minutes=$(((elapsed_time % 3600) / 60))
  seconds=$((elapsed_time % 60))
  printf "%02d:%02d:%02d" $hours $minutes $seconds
}


function copydb() {
  db=$1
  echo -e "\nStart processing db: '${db}'..\n"
  start_db_time=$(date +%s)

  # Delete the old DB from Impala cluster (if exists).
  impala-shell -i ${IMPALA_HOSTNAME} -q "drop database if exists ${db} cascade" |& tee error.log # impala-shell prints all logs in stderr, so wee need to capture them and put them in a file, in order to perform "grep" on them later
  log_errors=`cat error.log | grep -E "WARN|ERROR|FAILED"`
  if [ -n "$log_errors" ]; then
    echo -e "\n\nERROR: THERE WAS A PROBLEM WHEN DROPPING THE OLD DATABASE FROM IMPALA CLUSTER! EXITING...\n\n"
    rm -f error.log
    if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
      exit 2
    else
      return 2
    fi
  fi

  echo -e "\n\nCopying files of '${db}', from Ocean to Impala cluster..\n"
  start_file_transfer_time=$(date +%s)
  # Using max-bandwidth of: 70 * 150 Mb/s = 10.5 Gb/s
  # Using max memory of: 70 * 6144 = 430 Gb
  # Using 1MB as a buffer-size.
  # The " -Ddistcp.dynamic.recordsPerChunk=N" arg is not available in our version of hadoop
  # The "ug" args cannot be used as we get a "User does not belong to hive" error.
  # The "p" argument cannot be used, as it blocks the files from being used, giving a "sticky bit"-error, even after applying chmod and chown onm the files.
  hadoop distcp -Dmapreduce.map.memory.mb=6144 -m 70 -bandwidth 150 \
                -numListstatusThreads 40 \
                -copybuffersize 1048576 \
                -strategy dynamic \
                -blocksperchunk 8 \
                -pb \
                ${OCEAN_HDFS_NODE}/user/hive/warehouse/${db}.db ${IMPALA_HDFS_DB_BASE_PATH}

  if [ $? -eq 0 ]; then # Check the exit status of the "hadoop distcp" command.
    echo -e "\nSuccessfully copied the files of '${db}' from Ocean to Impala cluster, after: $(print_elapsed_time start_file_transfer_time)\n"
  else
    echo -e "\n\nERROR: FAILED TO TRANSFER THE FILES OF '${db}', WITH 'hadoop distcp'. GOT EXIT STATUS: $?\n\n"
    rm -f error.log
    if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
      exit 3
    else
      return 3
    fi
  fi

  # Give WRITE and EXECUTE permissions to the DBs' directory only, in order to be able to create more tables later, on top of that DB.
  hdfs dfs -conf ${IMPALA_CONFIG_FILE} -chmod u+wx ${IMPALA_HDFS_DB_BASE_PATH}/${db}.db
  # In case we ever use this script for a writable DB (using inserts/updates), we should perform the costly recursive operation as well, using the "-R" param.
  if [ $? -ne 0 ]; then # Check the exit status..
    echo -e "\n\nERROR: FAILED TO ASSIGN WRITE AND EXECUTE PERMISSIONS TO THE DIRECTORY OF DB: '${db}'. GOT EXIT STATUS: $?\n\n"
    rm -f error.log
    if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
      exit 4
    else
      return 4
    fi
  fi

  echo -e "\nCreating schema for db: '${db}'\n"
  start_create_schema_time=$(date +%s)

  # create the new database (with the same name)
  impala-shell -i ${IMPALA_HOSTNAME} -q "create database ${db}"

  # Because "Hive" and "Impala" do not have compatible schemas, we cannot use the "show create table <name>" output from hive to create the exact same table in impala.
  # So, we have to find at least one parquet file (check if it's there) from the table in the ocean cluster for impala to use it to extract the table-schema itself from that file.

  all_create_view_statements=()
  num_tables=0

  entities_on_ocean=(`hive -e "show tables in ${db};" | sed 's/WARN:.*//g'`)  # Get the tables and views without any potential the "WARN" logs.
  echo -e "\nGoing to create ${#entities_on_ocean[@]} entities for db '${db}'..\n"
  for i in ${entities_on_ocean[@]}; do # Use un-quoted values, as the elements are single-words.
    # Check if this is a view by showing the create-statement where it should print "create view" for a view, not the "create table". Unfortunately, there is no "show views" command.
    create_entity_statement=`hive --database ${db} -e "show create table ${i};"`  # We need to use the "--database", instead of including it inside the query, in order to return the statements with the '`' chars being in the right place to be used by impala-shell. However, we need to add the db-name in the "CREATE VIEW view_name" statement.
    create_view_statement_test=`echo -e "$create_entity_statement" | grep 'CREATE VIEW'`  # It needs to happen in two stages, otherwise the "grep" is not able to match multi-line statement.
    if [ -n "$create_view_statement_test" ]; then
      echo -e "\n'${i}' is a view, so we will save its 'create view' statement and execute it on Impala, after all tables have been created.\n"
      create_view_statement=`echo -e "$create_entity_statement" | sed 's/WARN:.*//g' | sed 's/"$/;/' | sed 's/^"//' | sed 's/\\"\\"/\"/g' | sed -e "${LOCATION_HDFS_NODE_SED_ARG}" | sed "s/CREATE VIEW /CREATE VIEW ${db}./"`
      all_create_view_statements+=("$create_view_statement")
    else
      echo -e "\n'${i}' is a table, so we will check for its parquet files and create the table on Impala cluster.\n"
      ((num_tables++))
      CURRENT_PRQ_FILE=`hdfs dfs -conf ${IMPALA_CONFIG_FILE} -ls -C "${IMPALA_HDFS_DB_BASE_PATH}/${db}.db/${i}/" | grep -v 'Found' | grep -v '_impala_insert_staging' |  head -1`
      if [ -z "$CURRENT_PRQ_FILE" ]; then # If there is not parquet-file inside.
          echo -e "\nERROR: THE TABLE \"${i}\" HAD NO FILES TO GET THE SCHEMA FROM! IT'S EMPTY!\n\n"
          if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
            exit 5
          fi  # This error is not FATAL, do we do not return from this function, in normal circumstances.
      else
        impala-shell -i ${IMPALA_HOSTNAME} -q "create table ${db}.${i} like parquet '${CURRENT_PRQ_FILE}' stored as parquet;" |& tee error.log
        log_errors=`cat error.log | grep -E "WARN|ERROR|FAILED"`
        if [ -n "$log_errors" ]; then
          echo -e "\n\nERROR: THERE WAS A PROBLEM WHEN CREATING TABLE '${i}'!\n\n"
          if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
            rm -f error.log
            exit 6
          fi  # This error is not FATAL, do we do not return from this function, in normal circumstances.
        fi
      fi
    fi
  done

  previous_num_of_views_to_retry=${#all_create_view_statements[@]}
  if [[ $num_tables -gt 0 ]]; then
    echo -e "\nAll ${num_tables} tables have been created, for db '${db}', going to create the ${previous_num_of_views_to_retry} views..\n"
  else
    echo -e "\nDB '${db}' does not have any tables, moving on to create the ${previous_num_of_views_to_retry} views..\n"
  fi

  if [[ $previous_num_of_views_to_retry -gt 0 ]]; then
    echo -e "\nAll_create_view_statements (${previous_num_of_views_to_retry}):\n\n${all_create_view_statements[@]}\n"  # DEBUG
  else
    echo -e "\nDB '${db}' does not contain any views.\n"
  fi

  level_counter=0
  while [[ $previous_num_of_views_to_retry -gt 0 ]]; do
    ((level_counter++))
    # The only accepted reason for a view to not be created, is if it depends on another view, which has not been created yet.
    # In this case, we should retry creating this particular view again.
    new_num_of_views_to_retry=0

    for create_view_statement in "${all_create_view_statements[@]}"; do # Here we use double quotes, as the elements are phrases, instead of single-words.
      impala-shell -i ${IMPALA_HOSTNAME} -q "${create_view_statement}" |& tee error.log # impala-shell prints all logs in stderr, so wee need to capture them and put them in a file, in order to perform "grep" on them later
      specific_errors=`cat error.log | grep -E "FAILED: ParseException line 1:13 missing TABLE at 'view'|ERROR: AnalysisException: Could not resolve table reference:"`
      if [ -n "$specific_errors" ]; then
        echo -e "\nspecific_errors: ${specific_errors}\n"
        echo -e "\nView '$(cat error.log | grep -Po "Query: CREATE VIEW ([^\s]+)" | sed 's/Query: CREATE VIEW //g')' failed to be created, possibly because it depends on another view.\n"
        ((new_num_of_views_to_retry++)) # Increment it here, instead of acquiring the array's size in the end, as that doesn't work for some reason.
      else
          all_create_view_statements=("${all_create_view_statements[@]/$create_view_statement}")  # Remove the current successful statement from the list.
          sleep 1 # Wait a bit for Impala to register that the view was created, before possibly referencing it by another view.
      fi
    done

    all_create_view_statements=("$(echo "${all_create_view_statements[@]}" | grep -v '^[\s]*$')")  # Re-index the array, filtering-out any empty elements.
    # Although the above command reduces the "active" elements to just the few to-be-retried, it does not manage to make the array return the its true size through the "${#all_create_view_statements[@]}" statement. So we use counters.

    if [[ $new_num_of_views_to_retry -eq $previous_num_of_views_to_retry ]]; then
      echo -e "\n\nERROR: THE NUMBER OF VIEWS TO RETRY HAS NOT BEEN REDUCED! THE SCRIPT IS LIKELY GOING TO AN INFINITE-LOOP! BREAKING-OUT..\n\n"
      if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
        exit 7
      else
        break # Break form the inf-loop of views and continue by computing stats for the tables.
      fi
    elif [[ $new_num_of_views_to_retry -gt 0 ]]; then
      echo -e "\nTo be retried \"create_view_statements\" (${new_num_of_views_to_retry}):\n\n${all_create_view_statements[@]}\n"
    else
      echo -e "\nFinished creating views, for db: '${db}', in level-${level_counter}.\n"
    fi
    previous_num_of_views_to_retry=$new_num_of_views_to_retry
  done

  entities_on_impala=(`impala-shell -i ${IMPALA_HOSTNAME} --delimited -q "show tables in ${db}"`)
  echo -e "\nThe schema of db '${db}', along with ${#entities_on_impala[@]} entities have been created, on Impala cluster, after: $(print_elapsed_time start_create_schema_time)\n"

  start_compute_stats_time=$(date +%s)
  echo -e "\nComputing stats for tables..\n"
  for i in ${entities_on_impala[@]}; do # Use un-quoted values, as the elemetns are single-words.
    # Taking the create table statement from the Ocean cluster, just to check if its a view, as the output is easier than using impala-shell from Impala cluster.
    create_view_statement=`hive -e "show create table ${db}.${i};" | grep "CREATE VIEW"`  # This grep works here, as we do not want to match multiple-lines.
    if [ -z "$create_view_statement" ]; then  # If it's a table, then go load the data to it.
      # Invalidate metadata of this DB's tables, in order for Impala to be aware of all parquet files put inside the tables' directories, previously, by "hadoop distcp".
      impala-shell -i ${IMPALA_HOSTNAME} -q "INVALIDATE METADATA ${db}.${i}"
      sleep 1
      impala-shell -i ${IMPALA_HOSTNAME} -q "compute stats ${db}.${i}" |& tee error.log
      log_errors=`cat error.log | grep -E "WARN|ERROR|FAILED"`
      if [ -n "$log_errors" ]; then
        echo -e "\n\nERROR: THERE WAS A PROBLEM WHEN COMPUTING STATS FOR TABLE '${i}'!\n\n"
        if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
          rm -f error.log
          exit 8
        fi  # This error is not FATAL, do we do not return from this function, in normal circumstances.
      fi
    fi
  done

  echo -e "\nFinished computing stats for tables, after: $(print_elapsed_time start_compute_stats_time)\n"
  rm -f error.log # Cleanup the temp log-file.

  # Check if the entities in both clusters are the same, down to the exact names, not just the counts. (they are sorted in the same way both in hive and impala)
  if [[ "${entities_on_impala[@]}" == "${entities_on_ocean[@]}" ]]; then
    echo -e "\nAll entities have been copied to Impala cluster.\n\nFinished processing db: '${db}', after: $(print_elapsed_time start_db_time)\n"
  else
    echo -e "\n\nERROR: $((${#entities_on_ocean[@]} - ${#entities_on_impala[@]})) ENTITIES OF DB '${db}' FAILED TO BE COPIED TO IMPALA CLUSTER!\n\n\nFinished processing db: '${db}', after: $(print_elapsed_time start_db_time)\n"
    if [[ SHOULD_EXIT_WHOLE_SCRIPT_UPON_ERROR -eq 1 ]]; then
      exit 9
    fi
  fi
}


MONITOR_DB=$1

copydb $MONITOR_DB'_institutions'
copydb $MONITOR_DB

