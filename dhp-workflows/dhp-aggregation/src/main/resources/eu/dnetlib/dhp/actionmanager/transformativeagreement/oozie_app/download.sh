#!/bin/bash
curl -L $1  | hdfs dfs -put - $2