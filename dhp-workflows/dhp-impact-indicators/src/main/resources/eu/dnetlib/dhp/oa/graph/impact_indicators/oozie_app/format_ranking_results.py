# This program reads hdfs directories containing ranking results from openaire's cluster.
# Based on the parameters provided by the user, it will create different types of output files.

# Modes available are:
# 1. bip 
# This will result in output of the form required for bip-finder's update. 
# Its lines conform to the following format:
# <doi> \t <pagerank> \t <pagerank_normalized> \t <attrank> \t <attrank_normalized> \t <citation_count> \t <citation_count_normalized> \t <3y_cc> \t <3y_cc_normalized> \t <tar_ram> \t <references_count>

# 2. zenodo
# This is the format used in zenodo for Bip-DB.  (6 way classes will be named C1, C2, ..., C6)
# This should output two files per ranking method with each line having the following data:
# a. <id> <score> <6-way-class>
# NOTE: this should also run for openaire-id files, hence we should have a total of 4 files per ranking (2 for each type of identifier)
# In 'zenodo' mode the user specifies only a single file, for which zenodo-based output will be created

# 3. json
# This if the format used to provide openAIRE / claudio with data containing 1 json per identifier
# An example of such a json format follows:
#{
#    "50|dedup_wf_001::08823c8f5c3ca2eae523817036cdda67": [
#       {
#            "id": "influence",
#            "unit": [
#                {
#                    "key": "score",
#                    "value": "5.06690394631e-09" 
#                },
#                {
#                    "key": "class",
#                    "value": "C" 
#                }
#            ]
#        },
#        {
#            "id": "popularity_alt",
#            "unit": [
#                {
#                    "key": "score",
#                    "value": "0.0" 
#                },
#                {
#                    "key": "class",
#                    "value": "C" 
#                }
#            ]
#        },
#        {
#            "id": "popularity",
#            "unit": [
#                {
#                    "key": "score",
#                    "value": "3.11855618382e-09" 
#                },
#                {
#                    "key": "class",
#                    "value": "C" 
#                }
#            ]
#        },
#        {
#            "id": "influence_alt",
#            "unit": [
#                {
#                    "key": "score",
#                    "value": "0.0" 
#                },
#                {
#                    "key": "class",
#                    "value": "C" 
#                }
#            ]
#        },
#        {
#            "id": "impulse",
#            "unit": [
#                {
#                    "key": "score",
#                    "value": "0.0" 
#                },
#                {
#                    "key": "class",
#                    "value": "C" 
#                }
#            ]
#        }
#    ]
#}


#################################################################################################
# Imports
import sys
import time

# Sparksession lib to communicate with cluster via session object
from pyspark.sql import SparkSession

# Import sql types to define the schema of score output files
from pyspark.sql.types import *

# Import sql functions with shorthand alias
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

# Json specific encoding
import json
#################################################################################################
# Clean up directory name
def clean_directory_name(dir_name):
	# We have a name with the form *_bip_universe<digits>_* or *_graph_universe<digits>_* 
	# and we need to keep the parts in *	
	dir_name_parts = dir_name.split('_')
	dir_name_parts = [part for part in dir_name_parts if ('bip' not in part and 'graph' not in part and 'universe' not in part and 'from' not in part)]
	
	clean_name = '_'.join(dir_name_parts)
	clean_name = clean_name.replace('_id', '_ids')
	
	clean_name = clean_name.replace('.txt', '')
	clean_name = clean_name.replace('.gz', '')
	
	if 'openaire_ids_' in clean_name:
		clean_name = clean_name.replace('openaire_ids_', '')
		clean_name = clean_name + '_openaire_ids.txt.gz'
	else:
		clean_name = clean_name + '.txt.gz/'
	
	return clean_name
# --------------------------------------------------------------------------------------------- #
# User defined function to escape special characters in a string that will turn into a json key
@udf(StringType())
def json_encode_key(doi_string):
	return json.dumps(doi_string)
#################################################################################################
# --------------------------------------------------------------------------------------------- #
# Arguments from command line and initializations

# Time initialization
start_time = time.time()

# Check whether input is correct, otherwise exit with appropriate message
if len(sys.argv) < 2:
	print ("Usage: ./format_ranking_results.py <mode> <input_file|input_file_list> <num_partitions>")
	sys.exit(0)
	
# Define valid modes:
valid_modes = ['json', 'zenodo', 'bip', 'json-5-way']
# Read mode provided by user
mode = sys.argv[1].strip()

# If mode isn't valid, exit
if mode not in valid_modes:
	print ("Usage: ./format_ranking_results.py <mode> <input_file|input_file_list> <num_partitions>\n")
	print ("Invalid mode provided. Valid modes: ['zenodo', 'bip', 'json', 'json-5-way']")
	sys.exit(0)


# Once here, we should be more or less okay to run.

# Define the spark session object
spark = SparkSession.builder.appName('Parse Scores - ' + str(mode) + ' mode').getOrCreate()
# Set Log Level for spark session
spark.sparkContext.setLogLevel('WARN')

# Here we define the schema shared by all score output files 
# - citation count variants have a slightly different schema, due to their scores being integers
float_schema = StructType([
	StructField('id', StringType(), False),
	StructField('score', FloatType(), False),
	StructField('normalized_score', FloatType(), False),
	StructField('3-way-class', StringType(), False),
	StructField('5-way-class', StringType(), False)
	])
	
int_schema = StructType([
	StructField('id', StringType(), False),
	StructField('score', IntegerType(), False),
	StructField('normalized_score', FloatType(), False),
	StructField('3-way-class', StringType(), False),
	StructField('5-way-class', StringType(), False)
	])
	
# This schema concerns the output of the file
# containing the number of references of each doi
refs_schema = StructType([
	StructField('id', StringType(), False),
	StructField('num_refs', IntegerType(), False),
	])
	
print("--- Initialization time: %s seconds ---" % (time.time() - start_time))

# --------------------------------------------------------------------------------------------- #

# Time the main program execution
start_time = time.time()

# The following is executed when the user requests the bip-update specific file
if mode == 'bip':

	# Read the remaining input files
	if len(sys.argv) < 8:
		print ("\n\nInsufficient input for 'bip' mode.")
		print ("File list required: <pagerank> <attrank> <citation count> <3-year citation count> <tar-ram> <number of references> <num_partitions>\n")
		sys.exit(0)
		
	
	# Read number of partitions: 
	num_partitions 	= int(sys.argv[-1])
		
		
	pagerank_dir 	= sys.argv[2]
	attrank_dir	= sys.argv[3]
	cc_dir		= sys.argv[4]
	impulse_dir	= sys.argv[5]
	ram_dir		= sys.argv[6]

	# NOTE: This was used initial, but @Serafeim told me to remove it since we don't get doi-doi referencew anymore
	# In case of emergency, bring this back
	# refs_dir	= sys.argv[7]	
		
	# Score-specific dataframe
	pagerank_df = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(pagerank_dir).repartition(num_partitions, 'id')
	attrank_df  = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(attrank_dir).repartition(num_partitions, 'id')
	cc_df	    = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(cc_dir).repartition(num_partitions, 'id')
	impulse_df   = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(impulse_dir).repartition(num_partitions, 'id')
	ram_df      = spark.read.schema(float_schema).option('delimiter', '\t').option('header', True).csv(ram_dir).repartition(num_partitions, 'id')
	# refs_df     = spark.read.schema(refs_schema).option('delimiter', '\t').option('header',True).csv(refs_dir).repartition(num_partitions, 'id')
	
	# ----------- TESTING CODE --------------- #
	# pagerank_entries = pagerank_df.count()
	# attrank_entries = attrank_df.count()
	# cc_entries = cc_df.count()
	# impulse_entries = impulse_df.count()
	# ram_entries = ram_df.count()
	# refs_entries = refs_df.count()
		
	# print ("Pagerank:" + str(pagerank_entries))
	# print ("AttRank:" + str(attrank_entries))
	# print ("CC entries: " + str(cc_entries))
	# print ("Impulse entries: " + str(impulse_entries))
	# print ("Refs: " + str(refs_entries))
	# ---------------------------------------- #
	
	# Create a new dataframe with the required data
	results_df  = pagerank_df.select('id', F.col('score').alias('pagerank'), F.col('normalized_score').alias('pagerank_normalized'))
	# Add attrank dataframe
	results_df  = results_df.join(attrank_df.select('id', 'score', 'normalized_score'), ['id'])\
				.select(results_df.id, 'pagerank', 'pagerank_normalized', F.col('score').alias('attrank'), F.col('normalized_score').alias('attrank_normalized'))

	# Add citation count dataframe
	results_df  = results_df.join(cc_df.select('id', 'score', 'normalized_score'), ['id'])\
				.select(results_df.id, 'pagerank', 'pagerank_normalized', 'attrank', 'attrank_normalized', F.col('score').alias('cc'), F.col('normalized_score').alias('cc_normalized'))

	# Add 3-year df
	results_df  = results_df.join(impulse_df.select('id', 'score', 'normalized_score'), ['id'])\
				.select(results_df.id, 'pagerank', 'pagerank_normalized', 'attrank', 'attrank_normalized', 'cc', 'cc_normalized', \
					F.col('score').alias('3-cc'), F.col('normalized_score').alias('3-cc_normalized'))
	
	# Add ram df
	results_df  = results_df.join(ram_df.select('id', 'score'), ['id'])\
				.select(results_df.id, 'pagerank', 'pagerank_normalized', 'attrank', 'attrank_normalized', 'cc', 'cc_normalized',\
					'3-cc', '3-cc_normalized', F.col('score').alias('ram'))
	
	# Add references - THIS WAS REMOVED SINCE WE DON't GET DOI REFERENCES
	# In case of emergency bring back
	# results_df  = results_df.join(refs_df, ['id']).select(results_df.id, 'pagerank', 'pagerank_normalized', 'attrank', 'attrank_normalized', \
	#						      'cc', 'cc_normalized', '3-cc', '3-cc_normalized', 'ram', 'num_refs')
	
	# Write resulting dataframe to file
	output_dir = "/".join(pagerank_dir.split('/')[:-1])
	output_dir = output_dir + '/bip_update_data.txt.gz'
	
	print("Writing to:" +  output_dir)
	results_df.write.mode('overwrite').option('delimiter','\t').option('header',True).csv(output_dir, compression='gzip')
	
# The following is executed when the user requests the zenodo-specific file
elif mode == 'zenodo':

	# Read the remaining input files
	if len(sys.argv) < 9:
		print ("\n\nInsufficient input for 'zenodo' mode.")
		print ("File list required: <pagerank> <attrank> <citation count> <3-year citation count> <tar-ram> <num_partitions> <graph_type>\n")
		sys.exit(0)
		
	# Read number of partitions: 
	num_partitions 	= int(sys.argv[-2])
	graph_type 	= sys.argv[-1]
	
	if graph_type not in ['bip', 'openaire']:
		graph_type = 'bip'		
		
	pagerank_dir 	= sys.argv[2]
	attrank_dir	= sys.argv[3]
	cc_dir		= sys.argv[4]
	impulse_dir	= sys.argv[5]
	ram_dir		= sys.argv[6]

	# Output directory is common for all files
	output_dir_prefix = "/".join(pagerank_dir.split('/')[:-1])
	# Method-specific outputs
	pagerank_output   = clean_directory_name(pagerank_dir.split('/')[-1])
	attrank_output    = clean_directory_name(attrank_dir.split('/')[-1])
	cc_output   	  = clean_directory_name(cc_dir.split('/')[-1])
	impulse_output    = clean_directory_name(impulse_dir.split('/')[-1])
	ram_output        = clean_directory_name(ram_dir.split('/')[-1])

	# --------- PageRank ----------- #
	# Get per file the doi - score - 6-way classes and write it to output
	print("Writing to: " +  output_dir_prefix + '/' + pagerank_output)
	pagerank_df = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(pagerank_dir).repartition(num_partitions, 'id').select('id', 'score', '5-way-class')
	# Replace dataframe class names
	pagerank_df = pagerank_df.withColumn('class', F.lit('C6'))
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('E'), F.lit('C5')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.drop('5-way-class')
	
	if graph_type == 'openaire':
		pagerank_df = pagerank_df.where( ~F.col('id').like('10.%') )
	
	# Write output
	pagerank_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_dir_prefix + '/' + pagerank_output, compression='gzip')
	# --------- AttRank ----------- #
	print("Writing to: " +  output_dir_prefix + '/' + attrank_output)	
	attrank_df  = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(attrank_dir).repartition(num_partitions, 'id').select('id', 'score', '5-way-class')
	# Replace dataframe class names
	attrank_df = attrank_df.withColumn('class', F.lit('C6'))
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('E'), F.lit('C5')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	attrank_df = attrank_df.drop('5-way-class')
	
	if graph_type == 'openaire':
		attrank_df = attrank_df.where( ~F.col('id').like('10.%') )
		
	# Write output
	attrank_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_dir_prefix + '/' + attrank_output, compression='gzip')	
	# --------- Citation Count ----------- #
	print("Writing to: " +  output_dir_prefix + '/' + cc_output)	
	cc_df	    = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(cc_dir).repartition(num_partitions, 'id').select('id', 'score', '5-way-class')
	# Replace dataframe class names
	cc_df = cc_df.withColumn('class', F.lit('C5'))
	# cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('E'), F.lit('C5')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	cc_df = cc_df.drop('5-way-class')
	
	if graph_type == 'openaire':
		cc_df = cc_df.where( ~F.col('id').like('10.%') )
			
	# Write output	
	cc_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_dir_prefix + '/' + cc_output, compression='gzip')	
	# --------- Impulse ----------- #
	print("Writing to: " +  output_dir_prefix + '/' + impulse_output)	
	impulse_df   = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(impulse_dir).repartition(num_partitions, 'id').select('id', 'score', '5-way-class')
	# Replace dataframe class names
	impulse_df = impulse_df.withColumn('class', F.lit('C5'))
	# impulse_df = impulse_df.withColumn('class', F.when(F.col('6-way-class') == F.lit('E'), F.lit('C5')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	impulse_df = impulse_df.drop('5-way-class')
	
	if graph_type == 'openaire':
		impulse_df = impulse_df.where( ~F.col('id').like('10.%') )
			
	# Write output	
	impulse_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_dir_prefix + '/' +  impulse_output, compression='gzip')	
	# --------- RAM ----------- #		
	print("Writing to: " +  output_dir_prefix + '/' + ram_output)	
	ram_df      = spark.read.schema(float_schema).option('delimiter', '\t').option('header', True).csv(ram_dir).repartition(num_partitions, 'id').select('id', 'score', '5-way-class')
	# Replace dataframe class names
	ram_df = ram_df.withColumn('class', F.lit('C5'))
	# ram_df = ram_df.withColumn('class', F.when(F.col('6-way-class') == F.lit('E'), F.lit('C5')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	ram_df = ram_df.drop('5-way-class')
	
	if graph_type == 'openaire':
		ram_df = ram_df.where( ~F.col('id').like('10.%') )
		
	# Write output		
	ram_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_dir_prefix + '/' + ram_output, compression='gzip')	

# The following produces the json file required by openaire	
elif mode == 'json':

	# Read the remaining input files
	if len(sys.argv) < 9:
		print ("\n\nInsufficient input for 'json' mode.")
		print ("File list required: <pagerank> <attrank> <citation count> <3-year citation count> <tar-ram> <num_partitions> <graph_type>\n")
		sys.exit(0)
		
	# Read number of partitions: 
	num_partitions 	= int(sys.argv[-2])
	graph_type 	= sys.argv[-1]
	
	if graph_type not in ['bip', 'openaire']:
		graph_type = 'bip'
	
	print ("Graph type: " + str(graph_type))	
	
	# File directories		
	pagerank_dir 	= sys.argv[2]
	attrank_dir	= sys.argv[3]
	cc_dir		= sys.argv[4]
	impulse_dir	= sys.argv[5]
	ram_dir		= sys.argv[6]
	
	print ("Reading files:")
	print (pagerank_dir)
	print (attrank_dir)
	print (cc_dir)
	print (impulse_dir)
	print (ram_dir)
	
	# Score-specific dataframe - read inputs
	pagerank_df = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(pagerank_dir).repartition(num_partitions, 'id')
	attrank_df  = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(attrank_dir).repartition(num_partitions, 'id')
	cc_df	    = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(cc_dir).repartition(num_partitions, 'id')
	impulse_df  = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(impulse_dir).repartition(num_partitions, 'id')
	ram_df      = spark.read.schema(float_schema).option('delimiter', '\t').option('header', True).csv(ram_dir).repartition(num_partitions, 'id')	
	# --- Join the data of the various scores --- #
	
	# Create json data for pagerank
	pagerank_df = pagerank_df.select('id', F.map_concat(
							F.create_map(F.lit('key'), F.lit('score')),
					       		F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
					       F.map_concat(
					       		F.create_map(F.lit('key'), F.lit('class')),
					       		F.create_map(F.lit('value'), F.col('3-way-class'))).alias('class_map'))
				       		
	pagerank_df = pagerank_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('influence_values') )
	pagerank_df = pagerank_df.select('id', F.create_map(F.lit('id'), F.lit('influence')).alias('id_map'), F.col('influence_values'))
	pagerank_df = pagerank_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('influence'))).alias('influence_key'), F.to_json(F.col('influence_values')).alias('influence_values') )
	pagerank_df = pagerank_df.select('id', F.expr('substring(influence_key, 0, length(influence_key)-1)').alias('influence_key'), 'influence_values')
	pagerank_df = pagerank_df.select('id', 'influence_key', F.expr('substring(influence_values, 2, length(influence_values))').alias('influence_values'))
	pagerank_df = pagerank_df.select('id', F.concat_ws(', ', F.col('influence_key'), F.col('influence_values')).alias('influence_json'))
			       
	# Create json data for attrank
	attrank_df = attrank_df.select('id', F.map_concat(
							F.create_map(F.lit('key'), F.lit('score')),
					       		F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
					       F.map_concat(
					       		F.create_map(F.lit('key'), F.lit('class')),
					       		F.create_map(F.lit('value'), F.col('3-way-class'))).alias('class_map'))
				       		
	attrank_df = attrank_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('popularity_values') )
	attrank_df = attrank_df.select('id', F.create_map(F.lit('id'), F.lit('popularity')).alias('id_map'), F.col('popularity_values'))
	attrank_df = attrank_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('popularity'))).alias('popularity_key'), F.to_json(F.col('popularity_values')).alias('popularity_values') )
	attrank_df = attrank_df.select('id', F.expr('substring(popularity_key, 0, length(popularity_key)-1)').alias('popularity_key'), 'popularity_values')
	attrank_df = attrank_df.select('id', 'popularity_key', F.expr('substring(popularity_values, 2, length(popularity_values))').alias('popularity_values'))
	attrank_df = attrank_df.select('id', F.concat_ws(', ', F.col('popularity_key'), F.col('popularity_values')).alias('popularity_json'))	
	
	# Create json data for CC
	cc_df = cc_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('3-way-class'))).alias('class_map'))
				       		
	cc_df = cc_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('influence_alt_values') )
	cc_df = cc_df.select('id', F.create_map(F.lit('id'), F.lit('influence_alt')).alias('id_map'), F.col('influence_alt_values'))
	cc_df = cc_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('influence_alt'))).alias('influence_alt_key'), F.to_json(F.col('influence_alt_values')).alias('influence_alt_values') )
	cc_df = cc_df.select('id', F.expr('substring(influence_alt_key, 0, length(influence_alt_key)-1)').alias('influence_alt_key'), 'influence_alt_values')
	cc_df = cc_df.select('id', 'influence_alt_key', F.expr('substring(influence_alt_values, 2, length(influence_alt_values))').alias('influence_alt_values'))
	cc_df = cc_df.select('id', F.concat_ws(', ', F.col('influence_alt_key'), F.col('influence_alt_values')).alias('influence_alt_json'))
	

	# Create json data for RAM	
	ram_df = ram_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('3-way-class'))).alias('class_map'))
				       		
	ram_df = ram_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('popularity_alt_values') )
	ram_df = ram_df.select('id', F.create_map(F.lit('id'), F.lit('popularity_alt')).alias('id_map'), F.col('popularity_alt_values'))
	ram_df = ram_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('popularity_alt'))).alias('popularity_alt_key'), F.to_json(F.col('popularity_alt_values')).alias('popularity_alt_values') )
	ram_df = ram_df.select('id', F.expr('substring(popularity_alt_key, 0, length(popularity_alt_key)-1)').alias('popularity_alt_key'), 'popularity_alt_values')
	ram_df = ram_df.select('id', 'popularity_alt_key', F.expr('substring(popularity_alt_values, 2, length(popularity_alt_values))').alias('popularity_alt_values'))
	ram_df = ram_df.select('id', F.concat_ws(', ', F.col('popularity_alt_key'), F.col('popularity_alt_values')).alias('popularity_alt_json'))
	
	# Create json data for impulse	
	impulse_df = impulse_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('3-way-class'))).alias('class_map'))
				       		
	impulse_df = impulse_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('impulse_values') )
	impulse_df = impulse_df.select('id', F.create_map(F.lit('id'), F.lit('impulse')).alias('id_map'), F.col('impulse_values'))
	impulse_df = impulse_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('impulse'))).alias('impulse_key'), F.to_json(F.col('impulse_values')).alias('impulse_values') )
	impulse_df = impulse_df.select('id', F.expr('substring(impulse_key, 0, length(impulse_key)-1)').alias('impulse_key'), 'impulse_values')
	impulse_df = impulse_df.select('id', 'impulse_key', F.expr('substring(impulse_values, 2, length(impulse_values))').alias('impulse_values'))
	impulse_df = impulse_df.select('id', F.concat_ws(', ', F.col('impulse_key'), F.col('impulse_values')).alias('impulse_json'))	
	
	#Join dataframes together
	results_df = pagerank_df.join(attrank_df, ['id'])
	results_df = results_df.join(cc_df, ['id'])
	results_df = results_df.join(ram_df, ['id'])
	results_df = results_df.join(impulse_df, ['id'])
	
	print ("Json encoding DOI keys")
	# Json encode doi strings
	results_df = results_df.select(json_encode_key('id').alias('id'), 'influence_json', 'popularity_json', 'influence_alt_json', 'popularity_alt_json', 'impulse_json')

	# Concatenate individual json columns
	results_df = results_df.select('id', F.concat_ws(', ', F.col('influence_json'), F.col('popularity_json'), F.col('influence_alt_json'), F.col('popularity_alt_json'), F.col('impulse_json') ).alias('json_data'))
	results_df = results_df.select('id', F.concat_ws('', F.lit('['), F.col('json_data'), F.lit(']')).alias('json_data') )
	
	# Filter out non-openaire ids if need
	if graph_type == 'openaire':
		results_df = results_df.where( ~F.col('id').like('"10.%') )

	# Concatenate paper id and add opening and ending brackets
	results_df = results_df.select(F.concat_ws('', F.lit('{'), F.col('id'), F.lit(': '), F.col('json_data'), F.lit('}')).alias('json') )

	# -------------------------------------------- #
	# Write json output - set the directory here
	output_dir = "/".join(pagerank_dir.split('/')[:-1])
	if graph_type == 'bip':
		output_dir = output_dir + '/bip_universe_doi_scores/'
	else:
		output_dir = output_dir + '/openaire_universe_scores/'

	# Write the dataframe
	print ("Writing output to: " + output_dir)
	results_df.write.mode('overwrite').option('header', False).text(output_dir, compression='gzip')

	# Rename the files to .json.gz now
	sc = spark.sparkContext
	URI = sc._gateway.jvm.java.net.URI
	Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
	FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
	# Get master prefix from input file path
	master_prefix = "/".join(pagerank_dir.split('/')[:5])
	fs = FileSystem.get(URI(master_prefix), sc._jsc.hadoopConfiguration())
	path = Path(output_dir)
	print ("Path is:" + path.toString())
	file_list = fs.listStatus(Path(output_dir))
	print ("Renaming files:")
	for f in file_list:
		initial_filename = f.getPath().toString()
		if "part" in initial_filename:
			print (initial_filename + " => " + initial_filename.replace(".txt.gz", ".json.gz"))
			fs.rename(Path(initial_filename), Path(initial_filename.replace(".txt.gz", ".json.gz")))


	'''
	DEPRECATED: 
	# -------------------------------------------- #
	# Write json output
	output_dir = "/".join(pagerank_dir.split('/')[:-1])
	if graph_type == 'bip':
		output_dir = output_dir + '/bip_universe_doi_scores_txt/'
	else:
		output_dir = output_dir + '/openaire_universe_scores_txt/'
		
	print ("Writing output to: " + output_dir)
	results_df.write.mode('overwrite').option('header', False).text(output_dir, compression='gzip')
	print ("Done writing first results")
	# Read results df as json and write it as json file
	print ("Reading json input from: " + str(output_dir))
	resulds_df_json = spark.read.json(output_dir).cache()
	# Write json to different dir
	print ("Writing json output to: " + output_dir.replace("_txt", ""))
	resulds_df_json.write.mode('overwrite').json(output_dir.replace("_txt", ""), compression='gzip')
	'''

# The following produces the json file required by openaire	
elif mode == 'json-5-way':

	# Read the remaining input files
	if len(sys.argv) < 9:
		print ("\n\nInsufficient input for 'json-5-way' mode.")
		print ("File list required: <pagerank> <attrank> <citation count> <3-year citation count> <tar-ram> <num_partitions> <graph_type>\n")
		sys.exit(0)
		
	# Read number of partitions: 
	num_partitions 	= int(sys.argv[-2])
	graph_type 	= sys.argv[-1]
	
	if graph_type not in ['bip', 'openaire']:
		graph_type = 'bip'	
	
	# File directories		
	pagerank_dir 	= sys.argv[2]
	attrank_dir	= sys.argv[3]
	cc_dir		= sys.argv[4]
	impulse_dir	= sys.argv[5]
	ram_dir		= sys.argv[6]
	
	# Score-specific dataframe - read inputs
	pagerank_df = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(pagerank_dir).repartition(num_partitions, 'id')
	attrank_df  = spark.read.schema(float_schema).option('delimiter', '\t').option('header',True).csv(attrank_dir).repartition(num_partitions, 'id')
	cc_df	    = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(cc_dir).repartition(num_partitions, 'id')
	impulse_df  = spark.read.schema(int_schema).option('delimiter', '\t').option('header',True).csv(impulse_dir).repartition(num_partitions, 'id')
	ram_df      = spark.read.schema(float_schema).option('delimiter', '\t').option('header', True).csv(ram_dir).repartition(num_partitions, 'id')	
	# --- Join the data of the various scores --- #
	
	
	# Replace 6-way classes with 5-way values
	pagerank_df = pagerank_df.withColumn('class', F.lit('C5'))
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	pagerank_df = pagerank_df.drop('5-way-class').withColumnRenamed('class', '5-way-class')
	
	
	# Create json data for pagerank
	pagerank_df = pagerank_df.select('id', F.map_concat(
							F.create_map(F.lit('key'), F.lit('score')),
					       		F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
					       F.map_concat(
					       		F.create_map(F.lit('key'), F.lit('class')),
					       		F.create_map(F.lit('value'), F.col('5-way-class'))).alias('class_map'))
					       		

				       		
	pagerank_df = pagerank_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('influence_values') )
	pagerank_df = pagerank_df.select('id', F.create_map(F.lit('id'), F.lit('influence')).alias('id_map'), F.col('influence_values'))
	pagerank_df = pagerank_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('influence'))).alias('influence_key'), F.to_json(F.col('influence_values')).alias('influence_values') )
	pagerank_df = pagerank_df.select('id', F.expr('substring(influence_key, 0, length(influence_key)-1)').alias('influence_key'), 'influence_values')
	pagerank_df = pagerank_df.select('id', 'influence_key', F.expr('substring(influence_values, 2, length(influence_values))').alias('influence_values'))
	pagerank_df = pagerank_df.select('id', F.concat_ws(', ', F.col('influence_key'), F.col('influence_values')).alias('influence_json'))
		
	# Replace 6-way classes with 5 way classes for attrank			       		
	attrank_df = attrank_df.withColumn('class', F.lit('C5'))
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	attrank_df = attrank_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	attrank_df = attrank_df.drop('5-way-class').withColumnRenamed('class', '5-way-class')		
			       
	# Create json data for attrank
	attrank_df = attrank_df.select('id', F.map_concat(
							F.create_map(F.lit('key'), F.lit('score')),
					       		F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
					       F.map_concat(
					       		F.create_map(F.lit('key'), F.lit('class')),
					       		F.create_map(F.lit('value'), F.col('5-way-class'))).alias('class_map'))
					       							       		
	attrank_df = attrank_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('popularity_values') )
	attrank_df = attrank_df.select('id', F.create_map(F.lit('id'), F.lit('popularity')).alias('id_map'), F.col('popularity_values'))
	attrank_df = attrank_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('popularity'))).alias('popularity_key'), F.to_json(F.col('popularity_values')).alias('popularity_values') )
	attrank_df = attrank_df.select('id', F.expr('substring(popularity_key, 0, length(popularity_key)-1)').alias('popularity_key'), 'popularity_values')
	attrank_df = attrank_df.select('id', 'popularity_key', F.expr('substring(popularity_values, 2, length(popularity_values))').alias('popularity_values'))
	attrank_df = attrank_df.select('id', F.concat_ws(', ', F.col('popularity_key'), F.col('popularity_values')).alias('popularity_json'))	
	
	# Replace 6-way classes with 5 way classes for attrank			       		
	cc_df = cc_df.withColumn('class', F.lit('C5'))
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	cc_df = cc_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	cc_df = cc_df.drop('5-way-class').withColumnRenamed('class', '5-way-class')		
		
	# Create json data for CC
	cc_df = cc_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('5-way-class'))).alias('class_map'))
				       		
	cc_df = cc_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('influence_alt_values') )
	cc_df = cc_df.select('id', F.create_map(F.lit('id'), F.lit('influence_alt')).alias('id_map'), F.col('influence_alt_values'))
	cc_df = cc_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('influence_alt'))).alias('influence_alt_key'), F.to_json(F.col('influence_alt_values')).alias('influence_alt_values') )
	cc_df = cc_df.select('id', F.expr('substring(influence_alt_key, 0, length(influence_alt_key)-1)').alias('influence_alt_key'), 'influence_alt_values')
	cc_df = cc_df.select('id', 'influence_alt_key', F.expr('substring(influence_alt_values, 2, length(influence_alt_values))').alias('influence_alt_values'))
	cc_df = cc_df.select('id', F.concat_ws(', ', F.col('influence_alt_key'), F.col('influence_alt_values')).alias('influence_alt_json'))
	
	# Replace 6-way classes with 5 way classes for attrank			       		
	ram_df = ram_df.withColumn('class', F.lit('C5'))
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	ram_df = ram_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	ram_df = ram_df.drop('5-way-class').withColumnRenamed('class', '5-way-class')		

	# Create json data for RAM	
	ram_df = ram_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('5-way-class'))).alias('class_map'))
				       		
	ram_df = ram_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('popularity_alt_values') )
	ram_df = ram_df.select('id', F.create_map(F.lit('id'), F.lit('popularity_alt')).alias('id_map'), F.col('popularity_alt_values'))
	ram_df = ram_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('popularity_alt'))).alias('popularity_alt_key'), F.to_json(F.col('popularity_alt_values')).alias('popularity_alt_values') )
	ram_df = ram_df.select('id', F.expr('substring(popularity_alt_key, 0, length(popularity_alt_key)-1)').alias('popularity_alt_key'), 'popularity_alt_values')
	ram_df = ram_df.select('id', 'popularity_alt_key', F.expr('substring(popularity_alt_values, 2, length(popularity_alt_values))').alias('popularity_alt_values'))
	ram_df = ram_df.select('id', F.concat_ws(', ', F.col('popularity_alt_key'), F.col('popularity_alt_values')).alias('popularity_alt_json'))

	# Replace 6-way classes with 5 way classes for attrank			       		
	impulse_df = impulse_df.withColumn('class', F.lit('C5'))
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('D'), F.lit('C4')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('C'), F.lit('C3')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('B'), F.lit('C2')).otherwise(F.col('class')) )
	impulse_df = impulse_df.withColumn('class', F.when(F.col('5-way-class') == F.lit('A'), F.lit('C1')).otherwise(F.col('class')) )
	impulse_df = impulse_df.drop('5-way-class').withColumnRenamed('class', '5-way-class')		
	
	# Create json data for impulse	
	impulse_df = impulse_df.select('id', F.map_concat(
						F.create_map(F.lit('key'), F.lit('score')),
					       	F.create_map(F.lit('value'), F.col('score'))).alias('score_map'),
				   F.map_concat(
					       	F.create_map(F.lit('key'), F.lit('class')),
					       	F.create_map(F.lit('value'), F.col('5-way-class'))).alias('class_map'))
				       		
	impulse_df = impulse_df.select('id', F.create_map(F.lit('unit'), F.array([F.col('score_map'), F.col('class_map')]) ).alias('impulse_values') )
	impulse_df = impulse_df.select('id', F.create_map(F.lit('id'), F.lit('impulse')).alias('id_map'), F.col('impulse_values'))
	impulse_df = impulse_df.select('id', F.to_json(F.create_map(F.lit('id'), F.lit('impulse'))).alias('impulse_key'), F.to_json(F.col('impulse_values')).alias('impulse_values') )
	impulse_df = impulse_df.select('id', F.expr('substring(impulse_key, 0, length(impulse_key)-1)').alias('impulse_key'), 'impulse_values')
	impulse_df = impulse_df.select('id', 'impulse_key', F.expr('substring(impulse_values, 2, length(impulse_values))').alias('impulse_values'))
	impulse_df = impulse_df.select('id', F.concat_ws(', ', F.col('impulse_key'), F.col('impulse_values')).alias('impulse_json'))	
	
	#Join dataframes together
	results_df = pagerank_df.join(attrank_df, ['id'])
	results_df = results_df.join(cc_df, ['id'])
	results_df = results_df.join(ram_df, ['id'])
	results_df = results_df.join(impulse_df, ['id'])
	
	print ("Json encoding DOI keys")
	# Json encode doi strings
	results_df = results_df.select(json_encode_key('id').alias('id'), 'influence_json', 'popularity_json', 'influence_alt_json', 'popularity_alt_json', 'impulse_json')

	# Concatenate individual json columns
	results_df = results_df.select('id', F.concat_ws(', ', F.col('influence_json'), F.col('popularity_json'), F.col('influence_alt_json'), F.col('popularity_alt_json'), F.col('impulse_json') ).alias('json_data'))
	results_df = results_df.select('id', F.concat_ws('', F.lit('['), F.col('json_data'), F.lit(']')).alias('json_data') )
	
	# Filter out non-openaire ids if need
	if graph_type == 'openaire':
		results_df = results_df.where( ~F.col('id').like('10.%') )
	
	# Concatenate paper id and add opening and ending brackets
	results_df = results_df.select(F.concat_ws('', F.lit('{'), F.col('id'), F.lit(': '), F.col('json_data'), F.lit('}')).alias('json') )

	# TEST output and count
	# results_df.show(20, False)
	# print ("Results #" + str(results_df.count()))
					
	# -------------------------------------------- #
	# Write json output
	# -------------------------------------------- #
	# Write json output - set the directory here
	output_dir = "/".join(pagerank_dir.split('/')[:-1])
	if graph_type == 'bip':
		output_dir = output_dir + '/bip_universe_doi_scores/'
	else:
		output_dir = output_dir + '/openaire_universe_scores/'

	# Write the dataframe
	print ("Writing output to: " + output_dir)
	results_df.write.mode('overwrite').option('header', False).text(output_dir, compression='gzip')

	# Rename the files to .json.gz now
	sc = spark.sparkContext
	URI = sc._gateway.jvm.java.net.URI
	Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
	FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
	# Get master prefix from input file path
	master_prefix = "/".join(pagerank_dir.split('/')[:5])
	fs = FileSystem.get(URI(master_prefix), sc._jsc.hadoopConfiguration())
	path = Path(output_dir)
	print ("Path is:" + path.toString())
	file_list = fs.listStatus(Path(output_dir))
	print ("Renaming files:")
	for f in file_list:
		initial_filename = f.getPath().toString()
		if "part" in initial_filename:
			print (initial_filename + " => " + initial_filename.replace(".txt.gz", ".json.gz"))
			fs.rename(Path(initial_filename), Path(initial_filename.replace(".txt.gz", ".json.gz")))	

# Close spark session
spark.stop()	
	
print("--- Main program execution time: %s seconds ---" % (time.time() - start_time))
print("--- Finished --- \n\n")

