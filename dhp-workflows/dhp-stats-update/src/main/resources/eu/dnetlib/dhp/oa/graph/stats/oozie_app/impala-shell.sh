export PYTHON_EGG_CACHE=/home/giorgos.alexiou/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

echo "getting file from " $3

hdfs dfs -copyToLocal $3

echo "running impala shell now"
impala-shell -d $1 -f $2
echo "impala shell finished"
rm $2
