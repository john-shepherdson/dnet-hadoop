# -*- coding: utf-8 -*-

import re
import unicodedata
import html
from unidecode import unidecode
import json   
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
#import pandas as pd 

def load_txt(file_path):
    with open(file_path, 'r',  encoding='utf-8') as file:
        list_ = [line.strip() for line in file]
        return list_
    
def load_pickled_dict(file_path): 
    with open(file_path, 'rb') as file: 
        pickled_dict = pickle.load(file) 
        return pickled_dict
    

def load_json(file_path): 
    with open(file_path, 'r') as json_file:
        json_dict = json.load(json_file)
        return json_dict
        
categ_string = 'Laboratory|Univ/Inst|Hospital|Foundation|Specific|Museum'

def replace_double_consonants(text):
    # This regex pattern matches any double consonant
    pattern = r'([bcdfghjklmnpqrstvwxyz])\1'
    # The replacement is the first captured group (the single consonant)
    result = re.sub(pattern, r'\1', text, flags=re.IGNORECASE)
    return result

remove_list = [replace_double_consonants(x) for x in load_txt('remove_list.txt')]
stop_words = load_txt('stop_words.txt')
university_terms = [replace_double_consonants(x) for x in load_txt('university_terms.txt')]
city_names = [replace_double_consonants(x) for x in load_txt('city_names.txt')]

categ_dicts = load_json('dix_categ.json')


def is_contained(s, w):
    words = s.split()  # Split the string 's' into a list of words
    for word in words:
        if word not in w:  # If a word from 's' is not found in 'w'
            return False  # Return False immediately
    return True  # If all words from 's' are found in 'w', return True

def starts_with_any(string, prefixes):
    for prefix in prefixes:
        if string.startswith(prefix):
            return [True, prefix]
    return False

def remove_leading_numbers(s):
    return re.sub(r'^\d+', '', s)

def remove_outer_parentheses(string):
    """Remove outer parentheses from the string if they enclose the entire string."""
    if string.startswith('(') and string.endswith(')'):
        return string[1:-1].strip()
    return string

def index_multiple_matchings(pairs):
    result_dict = {}
    
    r_list = [pair[3] for pair in pairs]
    modified_list = [item for sublist in r_list for item in sublist]
    r = len(list(set(modified_list)))
        
    for t in [pair[0] for pair in pairs]:
        key = t
        if key in result_dict and r>1:
            result_dict[key] += 1
            
        else:
            result_dict[key] = 1
   
    return result_dict

def avg_string(df, col):
    avg = [] 
    for i in range(len(df)):
        avg.append(sum(len(s) for s in df[col].iloc[i])/len(df[col].iloc[i]))
    return sum(avg)/len(avg)

def remove_stop_words(text):
    words = text.split()
    filtered_words = [word for word in words if word not in stop_words]
    return ' '.join(filtered_words)


def remove_parentheses(text):
   return re.sub(r'\([^()]*\)', '', text)


def replace_umlauts(text):
    normalized_text = unicodedata.normalize('NFKD', text)
    replaced_text = ''.join(c for c in normalized_text if not unicodedata.combining(c))
    return replaced_text

def protect_phrases(input_string, phrases):
    # Replace phrases with placeholders
    placeholder_map = {}
    for i, phrase in enumerate(phrases):
        placeholder = "__PLACEHOLDER_" + str(i) + "__"
        placeholder_map[placeholder] = phrase
        input_string = input_string.replace(phrase, placeholder)
    return input_string, placeholder_map

def restore_phrases(split_strings, placeholder_map):
    # Restore placeholders with original phrases
    restored_strings = []
    for s in split_strings:
        for placeholder, phrase in placeholder_map.items():
            s = s.replace(placeholder, phrase)
        restored_strings.append(s)
    return restored_strings

def replace_comma_spaces(text):
    return text.replace('  ', ' ').replace(' , ', ', ')

def split_string_with_protection(input_string, protected_phrases):
    # Step 1: Protect specific phrases
    input_string, placeholder_map = protect_phrases(input_string, protected_phrases)
    
    # Step 2: Split the string on specified delimiters
    split_strings = [s.strip() for s in re.split(r'[,;/]| – ', input_string) if s.strip()]
    
    # Step 3: Restore protected phrases
    split_strings = restore_phrases(split_strings, placeholder_map)
    
    return split_strings

protected_phrases1 =  [
    phrase.format(x=x)
    for x in city_names
    for phrase in [
        'university california, {x}',
    #    'university california , {x}',

        'university colege hospital, {x}',
    #    'university colege hospital , {x}',
        
        'national univ ireland, {x}',
    #    'national univ ireland , {x}',

        'national university ireland, {x}',
    #    'national university ireland , {x}',

        'university colege, {x}',
    #    'university colege , {x}',
        
        'university hospital, {x}', 
    #    'university hospital , {x}', 

        'imperial colege, {x}',
    #    'imperial colege , {x}'
        
        'city university, {x}', 
    #    'city university , {x}'

        
    ]
]



replacements = {'uni versity':'university',
                'univ ':'university ',
                'univercity':'university', 
                'universtiy':'university', 
                'univeristy':'university',
                'universirty':'university', 
                'universiti':'university', 
                'universitiy':'university',
                'universty' :'university',
                'univ col': 'university colege',
                'belfield, dublin': 'dublin',
                'balsbridge, dublin': 'dublin', #ballsbridge
                'earlsfort terrace, dublin': 'dublin',
                'bon secours hospital, cork' : 'bon secours hospital cork',
                'bon secours hospital, dublin' : 'bon secours hospital dublin',
                'bon secours hospital, galway' : 'bon secours hospital galway',
                'bon secours hospital, tralee' : 'bon secours hospital tralee',
                'bon secours health system' : 'bon secours hospital dublin',
                'bon secours hospital, glasnevin' : 'bon secours hospital dublin',
                'imperial colege science, technology medicine' : 'imperial colege science technology medicine',
                'ucl queen square institute neurology' : 'ucl, london',
                'ucl institute neurology' : 'ucl, london',
                'royal holoway, university london' : 'royal holoway universi london', #holloway
                'city, university london' : 'city universi london',
                'city university, london' : 'city universi london',
                'aeginition':'eginition',
                'national technical university, athens' : 'national technical university athens' 
            # 'harvard medical school' : 'harvard university'


    
}


def substrings_dict(string):
    # Split the input string and clean each substring
   # split_strings =  split_string_with_protection(string.replace('univ coll', 'university college').replace('belfield, dublin', 'dublin').replace('ballsbridge, dublin', 'dublin').replace('earlsfort Terrace, dublin', 'dublin'), protected_phrases1)
    
    for old, new in replacements.items():
        string = string.replace(old, new)
    split_strings = split_string_with_protection(string, protected_phrases1)
    
    # Define a set of university-related terms for later use


    dict_string = {}
    index = 0    
    for value in split_strings:
        
        # Check if the substring contains any university-related terms
        if not any(term in value.lower() for term in university_terms):
            # Apply regex substitutions for common patterns
   
            modified_value = re.sub(r'universi\w*', 'universi', value, flags=re.IGNORECASE)
            modified_value = re.sub(r'institu\w*', 'institu', modified_value, flags=re.IGNORECASE)
            modified_value = re.sub(r'centre*', 'center', modified_value, flags=re.IGNORECASE)
            modified_value = re.sub(r'\bsaint\b', 'st', modified_value, flags=re.IGNORECASE) 
            modified_value = re.sub(r'\btrinity col\b', 'trinity colege', modified_value, flags=re.IGNORECASE)
            modified_value = re.sub(r'\btechnische\b', 'technological', modified_value, flags=re.IGNORECASE)

            

            # Add the modified substring to the dictionary
                     
            dict_string[index] = modified_value.lower().strip()
            index += 1
       # elif 'universitetskaya' in value.lower():
       #     index += 1


            # Add the original substring to the dictionary
        else:
            dict_string[index] = value.lower().strip()
            index += 1
            
    return dict_string



def clean_string(input_string):
    # Temporarily replace " - " with a unique placeholder
    placeholder = "placeholder"
  #  input_string = input_string.replace(" - ", placeholder)
    input_string = input_string.replace(" – ", placeholder)

    # Unescape HTML entities and convert to lowercase
    input_string = replace_comma_spaces(remove_stop_words(replace_double_consonants(replace_umlauts(unidecode(remove_parentheses(html.unescape(input_string.lower())))))).strip())
    
    # Normalize unicode characters (optional, e.g., replace umlauts)
    input_string = unidecode(input_string)
    
    # Replace `/` and `–` with space (do not replace hyphen `-`)
    result = re.sub(r'[/\-]', ' ', input_string)
    
    # Replace "saint" with "st"
    result = re.sub(r'\bsaint\b', 'st', result)
    result = re.sub(r'\baghia\b', 'agia', result)

    
    # Remove characters that are not from the Latin alphabet, or allowed punctuation
    result = replace_comma_spaces(re.sub(r'[^a-zA-Z\s,;/]', '', result).strip())
    
    # Restore the " - " sequence from the placeholder
    result = result.replace(placeholder, " – ")
    
    # Replace consecutive whitespace with a single space
    result = re.sub(r'\s+', ' ', result)
    #result = result.replace('ss', 's')
    
    return result.strip()  # Strip leading/trailing spaces


def clean_string_facts(input_string):
    # Replace specified characters with space
    input_string = remove_stop_words(replace_umlauts(unidecode(remove_parentheses(html.unescape(input_string.lower())))))
    result = re.sub(r'[/\-,]', ' ', input_string)
    result = re.sub(r'\bsaint\b', 'st', result) 

    # Remove characters that are not from the Latin alphabet or numbers
    result = re.sub(r'[^a-zA-Z0-9\s;/-]', '', result)
    
    # Replace consecutive whitespace with a single space
    result = re.sub(r'\s+', ' ', result)
    
    return result
    
    
def str_radius_u(string):
    string = string.lower()
    radius = 3
    
    str_list = string.split()
    indices = []
    result = []

    for i, x in enumerate(str_list):
        if is_contained('univers',x):
            indices.append(i)
        # elif is_contained('coll',x):
        #     indices.append(i)
            
    for r0 in indices:
        lmin =max(0,r0-radius)
        lmax =min(r0+radius, len(str_list))
        s = str_list[lmin:lmax+1]
        
        result.append(' '.join(s))
    
    return result 


def str_radius_coll(string):
    string = string.lower()
    radius = 1
    
    str_list = string.split()
    indices = []
    result = []

    for i, x in enumerate(str_list):
        if is_contained('col',x):
            indices.append(i)
  
    for r0 in indices:
        lmin =max(0,r0-radius)
        lmax =min(r0+radius, len(str_list))
        s = str_list[lmin:lmax]
        
        result.append(' '.join(s))
    
    return result 


def str_radius_h(string):
    string = string.lower()
    radius = 3
    
    str_list = string.split()
    indices = []
    result = []

    for i, x in enumerate(str_list):
        if is_contained('hospital',x):
            indices.append(i)
            
    for r0 in indices:
        lmin =max(0,r0-radius-1)
        lmax =min(r0+radius, len(str_list))
        s = str_list[lmin:lmax]
        
        result.append(' '.join(s))
    
    return result 


def str_radius_c(string):
    string = string.lower()
    radius = 2
    
    str_list = string.split()
    indices = []
    result = []

    for i, x in enumerate(str_list):
        if is_contained('clinic',x) or is_contained('klinik',x):
            indices.append(i)
            
    for r0 in indices:
        lmin =max(0,r0-radius-1)
        lmax =min(r0+radius, len(str_list))
        s = str_list[lmin:lmax]
        
        result.append(' '.join(s))
    
    return result 

def str_radius_r(string):
    string = string.lower()
    radius = 2
    
    str_list = string.split()
    indices = []
    result = []

    for i, x in enumerate(str_list):
        if is_contained('research',x):
            indices.append(i)
            
    for r0 in indices:
        lmin =max(0,r0-radius-1)
        lmax =min(r0+radius, len(str_list))
        s = str_list[lmin:lmax]
        
        result.append(' '.join(s))
    
    return result 

def str_radius_spec(string):
    spec = False
    for x in string.split():
        try:
            if categ_dicts[x] == 'Specific':
                spec = True
                return x
        except:
            pass
    if spec == False:
        return string        
        

def avg_string(df, col):
    avg = [] 
    for i in range(len(df)):
        avg.append(sum(len(s) for s in df[col].iloc[i])/len(df[col].iloc[i]))
    return sum(avg)/len(avg)



        
                                
def shorten_keywords(affiliations_simple):
    affiliations_simple_n = []

    for aff in affiliations_simple:
        inner = []
        for str in aff:
            if 'universi' in str:
                inner.extend(str_radius_u(str))
            elif 'col' in str and 'trinity' in str:
                inner.extend(str_radius_coll(str))
            elif 'hospital' in str or 'hopita' in str:
                inner.extend(str_radius_h(str))
            elif 'clinic' in str or 'klinik' in str:
                inner.extend(str_radius_c(str))
            elif 'research council' in str:
                inner.extend(str_radius_r(str))
            else:
                inner.append(str_radius_spec(str))

        affiliations_simple_n.append(inner)

    return affiliations_simple_n

def shorten_keywords_spark(affiliations_simple):
    affiliations_simple_n = []

    for aff in affiliations_simple:
      
        if 'universi' in aff:
            affiliations_simple_n.extend(str_radius_u(aff))
        elif 'col' in aff and 'trinity' in aff:
            affiliations_simple_n.extend(str_radius_coll(aff))
        elif 'hospital' in aff or 'hopita' in aff:
            affiliations_simple_n.extend(str_radius_h(aff))
        elif 'clinic' in aff or 'klinik' in aff:
            affiliations_simple_n.extend(str_radius_c(aff))
        elif 'research council' in aff:
            affiliations_simple_n.extend(str_radius_r(aff))
        else:
            affiliations_simple_n.append(str_radius_spec(aff))


    return affiliations_simple_n


def refine(list_, affil):
    affil = affil.lower()
    
    ids = []
    
    for matched_org_list in list_:      
     
        id_list = []
        
        for matched_org in matched_org_list:
            
            if dix_mult[matched_org] == 'unique':
                id_list.append(dix_acad[matched_org])
            else:
                city_found = False
                for city in dix_city[matched_org]:
                    if city[0] in affil:
                        id_list.append(city[1])
                        city_found = True
                        break
        
                if not city_found:
                    country_found = False
                        
                    for country in dix_country[matched_org]:
                        if country[0] in  list(country_mapping.keys()):
                            print(country[0])
                            if country[0] in affil or country_mapping[country[0]][0] in affil or country_mapping[country[0]][0] in affil:
                                id_list.append(country[1])
                                country_found = True
                                break
                    
                            

                        elif country[0] in affil:
                            print('country found',country[0])
                        
                            id_list.append(country[1])
                            country_found = True
                            break

                    
                    
                    if not country_found:
                        id_list.append(dix_acad[matched_org])
           
                
        
        ids.append(id_list)
        return ids
    
def compute_cos(x,s):
    vectorizer = CountVectorizer()

    s_vector = vectorizer.fit_transform([s]).toarray() #Else we compute the similarity of s with the original affiiation name
    x_vector = vectorizer.transform([x]).toarray()

    # Compute similarity between the vectors
    return cosine_similarity(x_vector, s_vector)[0][0]


# def find_ror(string, simU, simG):
#     df = pd.DataFrame()
 
#     df['Unique affiliations'] = [[string.lower()]]
#     academia = create_df_algorithm(df)
    
 
#     result = Aff_Ids(len(academia), academia,dix_acad, dix_mult, dix_city, dix_country, simU,simG)
#     if len(result)>0:
         
#         dict_aff_open = {x: y for x, y in zip(result['Original affiliations'], result['Matched organizations'])}
#         dict_aff_id = {x: y for x, y in zip(result['Original affiliations'], result['unique ROR'])}
    
#         dict_aff_score = {}
#         for i in range(len(result)):
#             if type(result['Similarity score'].iloc[i]) == list:
#                 dict_aff_score[result['Original affiliations'].iloc[i]] = result['Similarity score'].iloc[i]
#             else:
#                 dict_aff_score[result['Original affiliations'].iloc[i]] = [result['Similarity score'].iloc[i]]
                

#         pids = []
#         for i in range(len(df)):
#             pidsi = []
#             for aff in df['Unique affiliations'].iloc[i]:
#                 if aff in list(dict_aff_id.keys()):
#                     pidsi = pidsi + dict_aff_id[aff]
#             # elif 'unmatched organization(s)' not in pidsi:
#             #     pidsi = pidsi + ['unmatched organization(s)']
#             pids.append(pidsi)
                    
                    
#         names = []
#         for i in range(len(df)):
#             namesi = []
#             for aff in df['Unique affiliations'].iloc[i]:
#                 if aff in list(dict_aff_open.keys()):
#                     try:
#                         namesi = namesi + dict_aff_open[aff]
#                     except TypeError:
#                         namesi = namesi + [dict_aff_open[aff]]
                    
#             names.append(namesi)
            
#         scores = []
#         for i in range(len(df)):
#             scoresi = []
#             for aff in df['Unique affiliations'].iloc[i]:
#                 if aff in list(dict_aff_score.keys()):
#                     scoresi = scoresi +  dict_aff_score[aff]
                    
#             scores.append(scoresi)
            
            
#         df['Matched organizations'] = names
#         df['ROR'] = pids
#         df['Scores'] = scores


       
#         def update_Z(row):
#             if len(row['ROR']) == 0 or len(row['Scores']) == 0:
#                 return []
            
#             new_Z = []
#             for ror, score in zip(row['ROR'], row['Scores']):
#                 entry = {'ROR_ID': ror, 'Confidence': score}
#                 new_Z.append(entry)
#             return new_Z

#         matching = df.apply(update_Z, axis=1)

#         df['Matchings'] = matching

        
#         return df['Matchings'].iloc[0]
#     else: 
#         return 'no result'