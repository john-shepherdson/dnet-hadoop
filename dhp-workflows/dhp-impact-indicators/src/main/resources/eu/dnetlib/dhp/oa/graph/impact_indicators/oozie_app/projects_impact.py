import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

if len(sys.argv) < 8:
    print("Usage: projects_impact.py <relations_folder> <influence_file> <popularity_file> <cc_file> <impulse_file> <num_partitions> <output_dir>")
    sys.exit(-1)

appName = 'Project Impact Indicators'
conf = SparkConf().setAppName(appName)
sc = SparkContext(conf = conf)
spark = SparkSession.builder.appName(appName).getOrCreate()
sc.setLogLevel('OFF')

# input parameters
relations_fd = sys.argv[1]
influence_fd = sys.argv[2]
popularity_fd = sys.argv[3]
cc_fd = sys.argv[4]
impulse_fd = sys.argv[5]
num_partitions = int(sys.argv[6])
output_dir = sys.argv[7]

# schema for impact indicator files
impact_files_schema = StructType([
    StructField('resultId', StringType(), False),
    StructField('score', IntegerType(), False),
    StructField('class', StringType(), False),
])

# list of impact indicators
impact_indicators = [
    ('influence', influence_fd, 'class'),
    ('popularity', popularity_fd, 'class'),
    ('impulse', impulse_fd, 'score'),
    ('citation_count', cc_fd, 'score')
]

'''
    * Read impact indicator file and return a dataframe with the following schema:
    *   resultId: String
    *   indicator_name: Integer
'''
def read_df(fd, indicator_name, column_name):
    return spark.read.schema(impact_files_schema)\
        .option('delimiter', '\t')\
        .option('header', False)\
        .csv(fd)\
        .select('resultId', F.col(column_name).alias(indicator_name))\
        .repartition(num_partitions, 'resultId')

# Print dataframe schema, first 5 rows, and count
def print_df(df):
    df.show(50)
    df.printSchema()
    print(df.count())

# Sets a null value to the column if the value is equal to the given value
def set_class_value_to_null(column, value):
    return F.when(column != value, column).otherwise(F.lit(None))

# load and filter Project-to-Result relations
print("Reading relations")
relations = spark.read.json(relations_fd)\
			.select(F.col('source').alias('projectId'), F.col('target').alias('resultId'), 'relClass', 'dataInfo.deletedbyinference', 'dataInfo.invisible')\
			.where( (F.col('relClass') == 'produces') \
				& (F.col('deletedbyinference') == "false")\
                & (F.col('invisible') == "false"))\
			.drop('deletedbyinference')\
			.drop('invisible')\
            .drop('relClass')\
			.repartition(num_partitions, 'resultId')

for indicator_name, fd, column_name in impact_indicators:

    print("Reading {} '{}' field from file".format(indicator_name, column_name))
    df = read_df(fd, indicator_name, column_name)

    # sets a zero value to the indicator column if the value is C5
    if (column_name == 'class'):
        df = df.withColumn(indicator_name, F.when(F.col(indicator_name).isin("C5"), 0).otherwise(1))

    # print_df(df)

    print("Joining {} to relations".format(indicator_name))

    # NOTE: we use inner join because we want to keep only the results that have an impact score
    # also note that all impact scores have the same set of results
    relations = relations.join(df, 'resultId', 'inner')\
        .repartition(num_partitions, 'resultId')

# uncomment to print non-null values count for each indicator
# for indicator_name, fd, column_name in impact_indicators:
#     print("Counting non null values for {}".format(indicator_name))
#     print(relations.filter(F.col(indicator_name).isNotNull()).count())

# sum the impact indicator values for each project
relations.groupBy('projectId')\
    .agg(\
        F.sum('influence').alias('numOfInfluentialResults'),\
        F.sum('popularity').alias('numOfPopularResults'),\
        F.sum('impulse').alias('totalImpulse'),\
        F.sum('citation_count').alias('totalCitationCount')\
    )\
    .write.mode("overwrite")\
    .json(output_dir, compression="gzip")