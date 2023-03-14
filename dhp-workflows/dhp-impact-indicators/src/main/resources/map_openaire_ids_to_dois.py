import json
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

if len(sys.argv) != 3:
    print("Usage: map_openaire_ids_to_dois.py <hdfs_src_dir> <hdfs_output_dir>")
    sys.exit(-1)

conf = SparkConf().setAppName('BIP!: Map OpenAIRE IDs to DOIs')
sc = SparkContext(conf = conf)
spark = SparkSession.builder.appName('BIP!: Map OpenAIRE IDs to DOIs').getOrCreate()
sc.setLogLevel('OFF')

src_dir = sys.argv[1]
output = sys.argv[2]

# src_dir = "/tmp/beta_provision/graph/21_graph_cleaned/"
# output = '/tmp/openaireid_to_dois/'

def transform(doc):
    
    # get publication year from 'doc.dateofacceptance.value'
    dateofacceptance = doc.get('dateofacceptance', {}).get('value')

    year = 0 
    
    if (dateofacceptance is not None):
        year = dateofacceptance.split('-')[0]

    # for each pid get 'pid.value' if 'pid.qualifier.classid' equals to 'doi'
    dois = [ pid['value'] for pid in doc.get('pid', [])  if (pid.get('qualifier', {}).get('classid') == 'doi' and pid['value'] is not None)]

    num_dois = len(dois)
    
    # exlcude openaire ids that do not correspond to DOIs
    if (num_dois == 0): 
        return None
        
    fields = [ doc['id'], str(num_dois), chr(0x02).join(dois), str(year) ]
    
    return '\t'.join([ v.encode('utf-8') for v in fields ])
    
docs = None

for result_type in ["publication", "dataset", "software", "otherresearchproduct"]:
    
    tmp = sc.textFile(src_dir + result_type).map(json.loads)
    
    if (docs is None):
        docs = tmp
    else:
        # append all result types in one RDD
        docs = docs.union(tmp)

docs = docs.filter(lambda d: d.get('dataInfo', {}).get('deletedbyinference') == False and d.get('dataInfo', {}).get('invisible') == False)

docs = docs.map(transform).filter(lambda d: d is not None)

docs.saveAsTextFile(output)
