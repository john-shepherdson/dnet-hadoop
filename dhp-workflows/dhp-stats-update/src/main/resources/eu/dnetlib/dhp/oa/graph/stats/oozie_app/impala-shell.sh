export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

echo "Getting file from " $3
hdfs dfs -copyToLocal $3

echo "Running impala shell make the new database visible"
impala-shell -q "INVALIDATE METADATA;"

echo "Running impala shell to compute new table stats"
impala-shell -d $1 -f $2
echo "Impala shell finished"
rm $2
