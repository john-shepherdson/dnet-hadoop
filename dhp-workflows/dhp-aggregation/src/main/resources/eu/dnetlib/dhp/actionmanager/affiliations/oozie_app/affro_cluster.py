import sys 
##import functions
from functions_cluster import *
from matching_cluster import *
from create_input_cluster import *
import json

dix_org = load_json('dix_acad.json')
dix_mult = load_json('dix_mult.json')
dix_city = load_json('dix_city.json')
dix_country = load_json('dix_country.json')

print('READY')

def affro(raw_aff_string):
    result = Aff_Ids(create_df_algorithm(raw_aff_string), dix_org, dix_mult, dix_city, dix_country,  0.5, 0.5 )
    return {'raw_affiliation_string':raw_aff_string, 'Matchings': [{'RORid':x[2], 'Confidence':x[1]} for x in result]}

#raw_aff = 'university of california, los angeles, university of athens, university of california, san diego, university of athens, greece'


# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: python affro_spark.py <string> <float1> <float2>")
#         sys.exit(1)
#
#     string_arg = sys.argv[1]
#    # float_arg1 = float(sys.argv[2])
#    # float_arg2 = float(sys.argv[3])
#
#     print(affro(string_arg))
