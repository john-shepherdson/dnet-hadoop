#!/usr/bin/python
# This program reads the openaire to doi mapping from the ${synonymFolder} of the workflow
# and uses this mapping to create doi-based score files in the format required by BiP! DB.
# This is done by reading each openaire-id based ranking file and joining the openaire based
# score and classes to all the corresponding dois.
#################################################################################################
# Imports
import sys

# Sparksession lib to communicate with cluster via session object
from pyspark.sql import SparkSession

# Import sql types to define schemas
from pyspark.sql.types import *

# Import sql functions with shorthand alias
import pyspark.sql.functions as F

from pyspark.sql.functions import max
# from pyspark.sql.functions import udf
#################################################################################################
#################################################################################################
# Clean up directory name - no longer needed in final workflow version
'''
def clean_directory_name(dir_name):
    # We have a name with the form *_bip_universe<digits>_* or *_graph_universe<digits>_* 
    # and we need to keep the parts in *	

    
    dir_name_parts = dir_name.split('_')
    dir_name_parts = [part for part in dir_name_parts if ('bip' not in part and 'graph' not in part and 'universe' not in part and 'from' not in part)]
    
    dir_name = dir_name.replace("openaire_id_graph", "openaire_ids")
    clean_name = dir_name + ".txt.gz"

    # clean_name = '_'.join(dir_name_parts)

    # if '_ids' not in clean_name:
    #     clean_name = clean_name.replace('id_', 'ids_')
        	
    # clean_name = clean_name.replace('.txt', '')
    # clean_name = clean_name.replace('.gz', '')

    # if 'openaire_ids_' in clean_name:
    #     clean_name = clean_name.replace('openaire_ids_', '')
        # clean_name = clean_name + '.txt.gz'
    # else:
        # clean_name = clean_name + '.txt.gz'
	
    return clean_name
'''
#################################################################################################
if len(sys.argv) < 3:
    print ("Usage: ./map_scores_to_dois.py <synonym_folder> <num_partitions> <score_file_1> <score_file_2> <...etc...>")
    sys.exit(-1)

# Read arguments
synonyms_folder = sys.argv[1]
num_partitions = int(sys.argv[2])
input_file_list = [argument.replace("_openaire_id_graph", "").replace("_openaire_id_graph_", "") + "_openaire_ids.txt.gz" for argument in sys.argv[3:]]
# input_file_list = [clean_directory_name(item) for item in input_file_list]

# Prepare output specific variables
output_file_list = [item.replace("_openaire_ids", "") for item in input_file_list]
output_file_list = [item + ".txt.gz" if not item.endswith(".txt.gz") else item for item in output_file_list]

# --- INFO MESSAGES --- #
print ("\n\n----------------------------")
print ("Mpping openaire ids to DOIs")
print ("Reading input from: " + synonyms_folder)
print ("Num partitions: " + str(num_partitions))
print ("Input files:" + " -- ".join(input_file_list))
print ("Output files: " + " -- ".join(output_file_list))
print ("----------------------------\n\n")
#######################################################################################
# We weill define the following schemas:
# --> the schema of the openaire - doi mapping file [string - int - doi_list] (the separator of the doi-list is a non printable character)
# --> a schema for floating point ranking scores [string - float - string]  (the latter string is the class)
# --> a schema for integer ranking scores [string - int - string]  (the latter string is the class)

float_schema = StructType([
	StructField('id', StringType(), False),
	StructField('score', FloatType(), False),
	StructField('class', StringType(), False)
	])
	
int_schema = StructType([
	StructField('id', StringType(), False),
	StructField('score', IntegerType(), False),
	StructField('class', StringType(), False)
	])
	
# This schema concerns the output of the file
# containing the number of references of each doi
synonyms_schema = StructType([
	StructField('id', StringType(), False),
	StructField('num_synonyms', IntegerType(), False),
    StructField('doi_list', StringType(), False),
	])
#######################################################################################
# Start spark session
spark = SparkSession.builder.appName('Map openaire scores to DOIs').getOrCreate()
# Set Log Level for spark session
spark.sparkContext.setLogLevel('WARN')
#######################################################################################
# MAIN Program

# Read and repartition the synonym folder - also cache it since we will need to perform multiple joins
synonym_df = spark.read.schema(synonyms_schema).option('delimiter', '\t').csv(synonyms_folder)
synonym_df = synonym_df.select('id',  F.split(F.col('doi_list'), chr(0x02)).alias('doi_list'))
synonym_df = synonym_df.select('id', F.explode('doi_list').alias('doi')).repartition(num_partitions, 'id').cache()

# TESTING
# print ("Synonyms: " + str(synonym_df.count()))
# print ("DF looks like this:" )
# synonym_df.show(1000, False)

print ("\n\n-----------------------------")
# Now we need to join the score files on the openaire-id with the synonyms and then keep
# only doi - score - class and write this to the output
for offset, input_file in enumerate(input_file_list):

    print ("Mapping scores from " + input_file)

    # Select correct schema
    schema = int_schema
    if "attrank" in input_file.lower() or "pr" in input_file.lower() or "ram" in input_file.lower():
        schema = float_schema
    
    # Load file to dataframe
    ranking_df = spark.read.schema(schema).option('delimiter', '\t').csv(input_file).repartition(num_partitions, 'id')

    # Get max score
    max_score = ranking_df.select(max('score').alias('max')).collect()[0]['max']
    print ("Max Score for " + str(input_file) + " is " + str(max_score))
   
    # TESTING
    # print ("Loaded df sample:")
    # ranking_df.show(1000, False)

    # Join scores to synonyms and keep required fields
    doi_score_df = synonym_df.join(ranking_df, ['id']).select('doi', 'score', 'class').repartition(num_partitions, 'doi').cache()
    # Write output
    output_file = output_file_list[offset]
    print ("Writing to: " + output_file)
    doi_score_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_file, compression='gzip')
    
    # Creata another file for the bip update process
    ranking_df = ranking_df.select('id', 'score', F.lit(F.col('score')/max_score).alias('normalized_score'), 'class', F.col('class').alias('class_dup'))
    doi_score_df = synonym_df.join(ranking_df, ['id']).select('doi', 'score', 'normalized_score', 'class', 'class_dup').repartition(num_partitions, 'doi').cache()
    output_file = output_file.replace(".txt.gz", "_for_bip_update.txt.gz")
    print ("Writing bip update to: " + output_file)
    doi_score_df.write.mode('overwrite').option('delimiter','\t').option('header',False).csv(output_file, compression='gzip')
 
    
    # Free memory?
    ranking_df.unpersist(True)

print ("-----------------------------")
print ("\n\nFinished!\n\n")








