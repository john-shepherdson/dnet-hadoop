#!bin/bash
curl -LSs $1 | hdfs dfs -put - $2/$3