#!/usr/bin/python3

# Create openaire id - openaire id graph from openaire data

#############################################################################################################
# Program proceeds as follows:
# 1. We read the input folder provided from hdfs. 
#    This contains subfolders with openaire graph objects and openaire graph relations
# 2. We select all openaire graph objects of interest. We filter out based on visibility 
#    and inference criteria. We also filter out based on the availability of publication year
# 3. Get reference type dataframes from openaire. Then filter each one of them based on the
#    existence of citing and cited in the above filtered dataset. Get only citations
#    produced by publication objects, or otherresearchproducts of types: 
#	 [TBD]
# 4. Get objects that don't appear in the relations (from those gathered in step 1) and add 
#	 them to the graph
# 5. Group relations by citing paper and do graph-specific formatting
#############################################################################################################
# ---------- Imports ------------- #
import sys
# import pyspark
# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# Functions to effectively handle data
# manipulation for DataFrames
import pyspark.sql.functions as F
# Diagnostics
from timeit import default_timer as timer
# from datetime import timedelta, datetime
# -------------------------------- #

if len(sys.argv) < 5:
	print ("Usage: ./create_openaire_ranking_graph.py <openaire_graph_data_folder> <current_year> <num_partitions> <output_folder>")
	sys.exit(0)

# Inputs will be:

# 1. Folder where openaire graph is stored
graph_folder = sys.argv[1]
# 2. Current year (this will be needed for filtering)
current_year = int(sys.argv[2])
# 3. Number of partitions
num_partitions = int(sys.argv[3])
# 4. where to write output
output_folder = sys.argv[4]

# Lists of results types we want to inclued in the citations
# valid_result_types = ['publication', 'other']
valid_result_types = ['publication']
# list of types in otherresearchproduct which are considered valid for citations
valid_other = ['']

# Create the spark session
spark = SparkSession.builder.appName('oa ranking graph creation').getOrCreate()
# Set context level logging to WARN
spark.sparkContext.setLogLevel("WARN")

############################################################################################################################
# 1. Get the research objects and filter based on conditions. 
#    These will also be the unique identifiers we should find in the final graph

# Initialize an empty dataframe
oa_objects_df = None

# There is a directory structure on hdfs under the provided path. 
# We need to parse data from the folders: ["publication", "dataset", "software", "otherresearchproduct"]
# which are rankable oa result objects.

# Loop subfolders
for sub_folder in ["publication", "dataset", "software", "otherresearchproduct"]:
	# Read the json data of the graph into a dataframe initially
	if not oa_objects_df:
		oa_objects_df = spark.read.json(graph_folder + "/" + sub_folder).select('id', 'resulttype.classname', 'datainfo.deletedbyinference', 'datainfo.invisible', F.year('dateofacceptance.value').alias('year'))
		oa_objects_df = oa_objects_df.where( 'datainfo.deletedbyinference = false'  ).where( 'datainfo.invisible = false' ).repartition(num_partitions, 'id').cache()
	# If we already have data, simply add more to it
	else:
		sub_df = spark.read.json(graph_folder + "/" + sub_folder).select('id', 'resulttype.classname','datainfo.deletedbyinference', 'datainfo.invisible', F.year('dateofacceptance.value').alias('year'))
		sub_df = sub_df.where( 'datainfo.deletedbyinference = false ' ).where( 'datainfo.invisible = false ').cache()
		# Add the data to the openaire objects dataframe
		oa_objects_df = oa_objects_df.union(sub_df).repartition(num_partitions, 'id').cache()
		# Clear memory
		sub_df.unpersist(True)

# Remove those records without year
oa_objects_df = oa_objects_df.where(F.col('year').isNotNull())


# Now replace years where > (current_year+1) with 0
oa_objects_df = oa_objects_df.withColumn('clean_year', F.when(F.col('year').cast('int') > (current_year+1), 0).otherwise(F.col('year')))\
			     .drop('year').withColumnRenamed('clean_year', 'year').repartition(num_partitions, 'id')

# -------------------------------------------------------------------- #
'''
# Some diagnostics
print ("Min and max years:" ) 
oa_objects_df.select(F.max('year')).show()
oa_objects_df.select(F.min('year')).show()

# This should be slow due to not repartitioning by year
print ("Distinct years:") 
oa_objects_df.select('year').distinct().sort(F.col('year')).show(5000, False)

# Show distinct values of deletedbyinference and invisible to ensure we have the correct data
print ("Distinct deleted by inference:")
oa_objects_df.select('deletedbyinference').distinct().show()
print ("Distinct invisible values:")
oa_objects_df.select('invisible').distinct().show()

# Output total count
print ("Total num of research objects: " + str(oa_objects_df.count()))
'''
# -------------------------------------------------------------------- #

# Keep only required fields - we still keep resulttype.classname to
# filter the citation relationships we consider valid
oa_objects_df = oa_objects_df.drop('deletedbyinference').drop('invisible').distinct().cache()

'''
print ("OA objects Schema:")
oa_objects_df.printSchema()
sys.exit(0)
'''
############################################################################################################################
# 2. Get the relation objects and filter them based on their existence in the oa_objects_df
#    NOTE: we are only interested in citations of type "cites"
#	 Further, we 

# Deprecated line
# references_df = spark.read.json(graph_folder + "/relation").select(F.col('source').alias('citing'), F.col('target').alias('cited'), 'relClass')\
# 			  .where( 'relClass = "References"' ).repartition(num_partitions, 'citing').drop('relClass')
# print ("References df has: " + str(references_df.count()) + " entries")		  

# Collect only valid citations i.e., invisible = false & deletedbyinference=false  
cites_df  = spark.read.json(graph_folder + "/relation")\
			.select(F.col('source').alias('citing'), F.col('target').alias('cited'), 'collectedfrom.value', 'relClass', 'dataInfo.deletedbyinference', 'dataInfo.invisible')\
			.where( (F.col('relClass') == "Cites") \
				& (F.col('dataInfo.deletedbyinference') == "false")\
                & (F.col('dataInfo.invisible') == "false"))\
				.drop('dataInfo.deletedbyinference').drop('dataInfo.invisible')\
				.drop('deletedbyinference').drop('invisible')\
				.repartition(num_partitions, 'citing').drop('relClass')\
				.withColumn('collected_lower', F.expr('transform(value, x -> lower(x))'))\
				.drop('collectedfrom.value')\
				.drop('value')\
				.where(
					(F.array_contains(F.col('collected_lower'), "opencitations"))
            | 		(F.array_contains(F.col('collected_lower'), "crossref"))
            | 		(F.array_contains(F.col('collected_lower'), "microsoft academic graph"))
				).drop('collected_lower')
# print ("Cited df has: " + str(cites_df.count()) + " entries")	 

# DEPRECATED 
# cited_by_df   = spark.read.json(graph_folder + "/relation").select(F.col('target').alias('citing'), F.col('source').alias('cited'), 'relClass')\
# 			.where( 'relClass = "IsCitedBy"' ).repartition(num_partitions, 'citing').drop('relClass')		
# print ("Cited by df has: " + str(cited_by_df.count()) + " entries")

# DEPRECATED			
# Keep only relations where citing and cited are in the oa_objects_df
# references_df = references_df.join(oa_objects_df.select('id'), references_df.citing == oa_objects_df.id).drop('id')
# references_df = references_df.repartition(num_partitions, 'cited').join(oa_objects_df.select('id'), references_df.cited == oa_objects_df.id).drop('id').distinct().repartition(num_partitions, 'citing').cache()
# print ("References df now has: " + str(references_df.count()) +  " entries")

cites_df = cites_df.join(oa_objects_df.select('id', 'classname'), cites_df.citing == oa_objects_df.id).where( F.col('classname').isin(valid_result_types) ).drop('id').drop('classname')
cites_df = cites_df.repartition(num_partitions, 'cited').join(oa_objects_df.select('id'), cites_df.cited == oa_objects_df.id).distinct().repartition(num_partitions, 'citing').cache()
# TODO: add here a clause filtering out the citations 
# originating from "other" types of research objects which we consider valid

# print ("Cites df now has: " + str(cites_df.count()) + " entries")

# DEPRECATED
# cited_by_df = cited_by_df.join(oa_objects_df.select('id'), cited_by_df.citing == oa_objects_df.id).drop('id')
# cited_by_df = cited_by_df.repartition(num_partitions, 'cited').join(oa_objects_df.select('id'), cited_by_df.cited == oa_objects_df.id).drop('id').distinct().repartition(num_partitions, 'citing').cache()
# print ("Cited BY df now has: " + str(cited_by_df.count()) + " entries")

# DEPRECATED
# Join all the above into a single set
# citations_df = references_df.union(cites_df).distinct().repartition(num_partitions, 'citing').cache()
# Free space
# references_df.unpersist(True)
# cites_df.unpersist(True)

# citations_df = citations_df.union(cited_by_df).distinct().repartition(num_partitions, 'citing').cache()

# ALL citations we keep are in the cited_df dataframe
citations_df = cites_df

'''
# Show schema
print ("Citation schema:")
citations_df.printSchema()
print ("Objects schema:")
oa_objects_df.printSchema()
'''

# Free space
# cited_by_df.unpersist(True)

# Show total num of unique citations
'''
num_unique_citations = citations_df.count()
print ("Total unique citations: " + str(num_unique_citations))
'''
############################################################################################################################
# 3. Get any potentially missing 'citing' papers from references (these are dangling nodes w/o any outgoing references)
dangling_nodes = oa_objects_df.join(citations_df.select('citing').distinct(), citations_df.citing == oa_objects_df.id, 'left_anti')\
			      .select(F.col('id').alias('citing')).withColumn('cited', F.array([F.lit("0")])).repartition(num_partitions, 'citing')
# Count dangling nodes
'''
dangling_num = dangling_nodes.count()
print ("Number of dangling nodes: " + str(dangling_num))
'''
# print ("Dangling nodes sample:")
# dangling_nodes.show(10, False)
############################################################################################################################
# 4. Group the citation dataframe by citing doi, and create the cited dois list. Add dangling nodes to the result
graph = citations_df.groupBy('citing').agg(F.collect_set('cited').alias('cited')).repartition(num_partitions, 'citing').cache()
# Free space
citations_df.unpersist(True)

'''
num_nodes = graph.count()
print ("Entries in graph before dangling nodes:"  + str(num_nodes))
'''
# print ("Sample in graph: ")
# graph.show(10, False)

# Add dangling nodes
graph = graph.union(dangling_nodes).repartition(num_partitions, 'citing')
# Count current number of results
num_nodes = graph.count()
print ("Num entries after adding dangling nodes: " + str(num_nodes))

# Add publication year
graph = graph.join(oa_objects_df, graph.citing == oa_objects_df.id).select('citing', 'cited', 'year').cache()
num_nodes_final = graph.count()
print ("After adding year: " + str(num_nodes_final))
# print ("Graph sample:")
# graph.show(20, False)
# Calculate initial score of nodes (1/N)
initial_score = float(1)/float(num_nodes_final)
############################################################################################################################
# 5. Write graph to output file!
print("Writing output to: " + output_folder)

graph.select('citing', F.concat_ws("|", F.concat_ws(",",'cited'), F.when(F.col('cited').getItem(1) != "0", F.size('cited')).otherwise(F.lit("0")), F.lit(str(initial_score)) ).alias('cited'), 'year').withColumn('prev_pr', F.lit("0")).select('citing', 'cited', 'prev_pr', 'year')\
	.write.mode("overwrite").option("delimiter","\t").csv(output_folder, compression="gzip")

if num_nodes_final != num_nodes:
	print ("WARNING: the number of nodes after keeping only nodes where year is available went from: " + str(num_nodes) + " to " + str(num_nodes_final) + "\n")
	print ("Check for any mistakes...")

############################################################################################################################
print ("\nDONE!\n\n")
# Wrap up
spark.stop()
