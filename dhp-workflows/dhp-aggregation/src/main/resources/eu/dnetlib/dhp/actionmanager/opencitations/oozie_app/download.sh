#!/bin/bash
for file in $(echo $1 | tr ";" "\n"); do curl -L $(echo $file | cut -d '@' -f 1 ) | hdfs dfs -put - $2/$(echo $file | cut -d '@' -f 2)  ; done;