#!/bin/bash
curl -LSs $1 | hdfs dfs -put - $2/$3
#curl -LSs  http://api.crossref.org/works/10.1099/jgv.0.001453 > prova.txt