#/usr/bin/bash

# Read log files from ranking scripts and create a two-line file  
# with score limits for the various measures. To be used by Kleanthis

attrank_file=$(ls *attrank*.log);
pr_file=$(ls *pagerank*.log)
ram_file=$(ls *ram*.log);
cc_file=$(ls *cc*.log);
impulse_file=$(ls *impulse*.log);

echo
echo "-----------------------------"
echo "Attrank file:${attrank_file}";
echo "PageRank file:${pr_file}";
echo "RAM file:${ram_file}";
echo "CC file:${cc_file}";
echo "Impulse file:${impulse_file}";
echo "-----------------------------"
echo
echo

# output file will be called score_limits.csv
echo -e "influence_top001\tinfluence_top01\tinfluence_top1\tinfluence_top10\tpopularity_top001\tpopularity_top01\tpopularity_top1\tpopularity_top10\timpulse_top001\timpulse_top01\timpulse_top1\timpulse_top10\tcc_top001\tcc_top01\tcc_top1\tcc_top10" > score_limits.csv
# ---------------------------------------------------- #
# Get respective score limits (we don't need RAM)
inf_001=$(grep "^0.01%" ${pr_file} | cut -f 2);
inf_01=$(grep "^0.1%" ${pr_file} | cut -f 2);
inf_1=$(grep "^1%" ${pr_file} | cut -f 2);
inf_10=$(grep "^10%" ${pr_file} | cut -f 2);
echo "Influnence limits:"
echo -e "${inf_001}\t${inf_01}\t${inf_1}\t${inf_10}";
# ---------------------------------------------------- #
pop_001=$(grep "^0.01%" ${attrank_file} | cut -f 2);
pop_01=$(grep "^0.1%" ${attrank_file} | cut -f 2);
pop_1=$(grep "^1%" ${attrank_file} | cut -f 2);
pop_10=$(grep "^10%" ${attrank_file} | cut -f 2);
echo "Popularity limits:";
echo -e "${pop_001}\t${pop_01}\t${pop_1}\t${pop_10}";
# ---------------------------------------------------- #
imp_001=$(grep "^0.01%" ${impulse_file} | cut -f 2);
imp_01=$(grep "^0.1%" ${impulse_file} | cut -f 2);
imp_1=$(grep "^1%" ${impulse_file} | cut -f 2);
imp_10=$(grep "^10%" ${impulse_file} | cut -f 2);
echo "Popularity limits:";
echo -e "${imp_001}\t${imp_01}\t${imp_1}\t${imp_10}";
# ---------------------------------------------------- #
cc_001=$(grep "^0.01%" ${cc_file} | cut -f 2);
cc_01=$(grep "^0.1%" ${cc_file} | cut -f 2);
cc_1=$(grep "^1%" ${cc_file} | cut -f 2);
cc_10=$(grep "^10%" ${cc_file} | cut -f 2);
echo "Popularity limits:";
echo -e "${cc_001}\t${cc_01}\t${cc_1}\t${cc_10}";
# ---------------------------------------------------- #

echo -e "${inf_001}\t${inf_01}\t${inf_1}\t${inf_10}\t${pop_001}\t${pop_01}\t${pop_1}\t${pop_10}\t${imp_001}\t${imp_01}\t${imp_1}\t${imp_10}\t${cc_001}\t${cc_01}\t${cc_1}\t${cc_10}" >> score_limits.csv

echo
echo "score_limits.csv contents:"
cat score_limits.csv

echo;
echo;
