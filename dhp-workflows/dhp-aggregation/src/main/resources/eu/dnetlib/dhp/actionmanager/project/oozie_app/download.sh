#!/bin/bash
hdfs dfs -rm $2
curl -LSs $1 |  hdfs dfs -put - $2