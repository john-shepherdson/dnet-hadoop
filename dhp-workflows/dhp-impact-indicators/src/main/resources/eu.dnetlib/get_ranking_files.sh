ranking_results_folder=$1;

pr_file=`hdfs dfs -ls ${ranking_results_folder}/ | grep "/PR_.*" | grep -o "PR.*"`;
attrank_file=`hdfs dfs -ls ${ranking_results_folder}/ | grep "/AttRank.*" | grep -o "AttRank.*"`;
cc_file=`hdfs dfs -ls ${ranking_results_folder}/ | grep "/CC_.*" | grep -o "CC.*"`;
impulse_file=`hdfs dfs -ls ${ranking_results_folder}/ | grep "/3-year_.*" | grep -o "3-year.*"`;
ram_file=`hdfs dfs -ls ${ranking_results_folder}/ | grep "/RAM_.*" | grep -o "RAM.*"`;

echo "pr_file=${pr_file}";
echo "attrank_file=${attrank_file}";
echo "cc_file=${cc_file}";
echo "impulse_file=${impulse_file}";
echo "ram_file=${ram_file}";
# echo "TEST=`hdfs dfs -ls ${ranking_results_folder}/`";
