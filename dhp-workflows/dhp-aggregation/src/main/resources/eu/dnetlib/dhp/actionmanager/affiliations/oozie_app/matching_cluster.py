from collections import defaultdict
from collections import Counter

import Levenshtein

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from functions_cluster import *
from create_input_cluster import *

def best_sim_score(light_raw, l2, l3, l4, simU, simG):
    """
    Finds the best match between a 'key word' and several legal names from the OpenAIRE database.
    ---> corrects special cases in the main map that follows

    Args:
        l1: List of level2 affiliations.
        l2: number of candidates.
        l3: List of pairs.
        l4: mult

    Returns:
        List: Resulting list containing OpenAIRE names and their similarity scores.
    """
    
    vectorizer = CountVectorizer()
    numUniv = light_raw.lower().count('univ') 
    result = []
    best = [] 
    s = light_raw
    for j in range(len(l3)):
        x = l3[j][1] 
        
        if [x, l3[j][2]] in result:
            continue
        
        if l4[l3[j][0]] == 1:
            
            if  is_contained('univ', x.lower()) and  l3[j][2]> simU:
                result.append([x, l3[j][2]])
            elif  l3[j][2] >simG:
                result.append([x, l3[j][2]])

            
            
        elif l3[j][2] >=0.98:# and (is_contained("univ", x.lower()) or is_contained("college", x.lower()) or  is_contained("center", x.lower()) or  is_contained("schule", x.lower())): # If the similarity score of a pair (s,x) was 1, we store it to results list
            result.append([l3[j][1], 1])
            
        else:
            try:
                if not is_contained("univ", x.lower()):
                    continue  # Skip if x does not contain "university" or "univ"
                
                #  if (is_contained('hosp', x.lower()) and not is_contained('hosp', s)) or (not is_contained('hosp', x.lower()) and is_contained('hosp', s)) or (is_contained('hopital', x.lower()) and not is_contained('hopital', s)) or (not is_contained('hopital', x.lower()) and is_contained('hopital', s)):
                #      continue
                s_vector = vectorizer.fit_transform([s]).toarray() #Else we compute the similarity of s with the original affiiation name
                x_vector = vectorizer.transform([x]).toarray()
    
                # Compute similarity between the vectors
                similarity = cosine_similarity(x_vector, s_vector)[0][0]
                if similarity> 0.1:
                    similarity_l = 1 - Levenshtein.distance(x, l3[j][0]) / max(len(x), len(l3[j][0]))


                    best.append([x, similarity,similarity_l])#(similarity+similarity2)/2])
            except:
                KeyError
                    
    if best:
        # max_numbers = defaultdict(float)


# Assuming best is a list of three-element lists
# Each element is (string, number1, number2)
        max_numbers = defaultdict(float)
        for item in best:
            string, number1, number2 = item  # Unpack the three elements
            max_numbers[string] = max(max_numbers[string], number1)

        reduced_best = [[string, number1, number2] for string, number1, number2 in best if number1 == max_numbers[string]]

# Sort by number1 decreasingly and then by number2 in descending order
        reduced_best.sort(key=lambda x: (x[1], x[2]), reverse=True)

        result = result + reduced_best
                
    univ_list = []
    other_list = []
    
    for r in result:
        if is_contained('univ',r[0]):
            univ_list.append(r)
        else:
            other_list.append(r)
    
    limit =  min(numUniv, l2)

    if len(univ_list)> limit:
        result = univ_list[:limit] + other_list
        
    result_dict = {}
    pairs_dict = {}
    
    
    for l in l3:
        pairs_dict[l[1]] = l[2]
        
        
    for p in result:
        result_dict[p[0]]= pairs_dict[p[0]]
        
    
        
        
    result_dict_list = [[y[0],result_dict[y[0]]] for y in result]  
        
                
    return result_dict_list



    
    
def Aff_Ids(input, dix_org, dix_mult, dix_city_ror, dix_country_ror, simU, simG):
    
    """
    Matches affiliations in DataFrame 'DF' with names from dictionary 'dix_org' and their ROR_ids based on similarity scores.

    Args:
        m (int): The number of DOIs to check.
        DF (DataFrame): The input DataFrame containing affiliation data.
        dix_org (dict): A dictionary of names of organizations and their ROR_ids.
        simU (float): Similarity threshold for universities.
        simG (float): Similarity threshold for non-universities.

    Returns:
        DataFrame: The final DataFrame with matched affiliations and their corresponding similarity scores.
    """
    df_list = input[1]
    light_aff = input[0]
    vectorizer = CountVectorizer()

    lnamelist = list(dix_org.keys())
    dix = {}    # will store indeces and legalnames of organizations of the DOI { i : [legalname1, legalname2,...]}
    #pairs = [] 
    result = {}
    pairs = []
    
 
    def get_keywords(filtered_list):
        # Extract the "keywords" values from the dictionaries in filtered_list
        keywords_list = [entry["keywords"] for entry in filtered_list]
        
        return keywords_list
    keywords = get_keywords(df_list)


    for k,s in enumerate(keywords):
        similar_k = []
        pairs_k = []

        if s in lnamelist:
            similarity = 1
            similar_k.append(similarity)
            
            pairs_k.append((s,s,similarity,dix_org[s]))
            pairs.append((s,s,similarity,dix_org[s]))


            if k not in dix:
                dix[k] = [s]
            else:
                dix[k].append(s)
        else:

            for x in lnamelist:
                if  is_contained(s, x):

                    x_vector = vectorizer.fit_transform([x]).toarray()
                    s_vector = vectorizer.transform([s]).toarray()

                    # Compute similarity between the vectors
                    similarity = cosine_similarity(x_vector, s_vector)[0][0]
                    if similarity > min(simU, simG):
                        if (is_contained('univ', s) and is_contained('univ', x)) and similarity > simU:
                            similar_k.append(similarity)
                            pairs_k.append((s,x,similarity,dix_org[x]))
                            pairs.append((s,x,similarity,dix_org[x]))


                            if k not in dix:
                                dix[k] = [x]
                            else:
                                dix[k].append(x)
                        elif (not is_contained('univ', s) and not is_contained('univ', x)) and similarity > simG:
                            similar_k.append(similarity)
                            pairs_k.append((s,x,similarity,dix_org[x]))
                            pairs.append((s,x,similarity,dix_org[x]))


                            if k not in dix:
                                dix[k] = [x]
                            else:
                                dix[k].append(x)
                                
                elif is_contained(x, s):
                    if (is_contained('univ', s) and is_contained('univ', x)):

                        s_vector = vectorizer.fit_transform([s]).toarray()
                        x_vector = vectorizer.transform([x]).toarray()

                        # Compute similarity between the vectors
                        similarity = cosine_similarity(s_vector, x_vector)[0][0]
                        if similarity > simU: #max(0.82,sim):
                            similar_k.append(similarity)
                            pairs_k.append((s,x,similarity,dix_org[x]))
                            pairs.append((s,x,similarity,dix_org[x]))

                            if k not in dix:
                                dix[k] = [x]
                            else:
                                dix[k].append(x)
                    elif not is_contained('univ', s) and not is_contained('univ', x):

                        s_vector = vectorizer.fit_transform([s]).toarray()
                        x_vector = vectorizer.transform([x]).toarray()

                        # Compute similarity between the vectors
                        similarity = cosine_similarity(s_vector, x_vector)[0][0]
                        if similarity > simG: #max(0.82,sim):
                            similar_k.append(similarity)
                            pairs_k.append((s,x,similarity,dix_org[x]))
                            pairs.append((s,x,similarity,dix_org[x]))

                            if k not in dix:
                                dix[k] = [x]
                            else:
                                dix[k].append(x)  

        result[k] = pairs_k
        
    multi = index_multiple_matchings(list(set(pairs)))
   # need_check = list(set([i for i in range(len(multi)) if list(multi.values())[i]>1]))
   # print('here', multi)
   # need_check_keys = [keywords[i] for i in range(len(keywords)) if multi[keywords[i]]>1]
    need_check_keys = []
    for i in range(len(keywords)):
        try: 
            if  multi[keywords[i]]>1:
                need_check_keys.append(keywords[i])
        except:
            pass
        
    best =  best_sim_score(light_aff, len(keywords), pairs, multi, simU, simG) 
    matched_org = [x[0] for x in best]
  #      best_o = []
 #       best_s = []
  #      best_result = []
   #     for x in best:
    #        best_o.append([x[i][0]  for i in range(len(x))])
     #       best_s.append([round(x[i][1],2)  for i in range(len(x))])
      #  num_mathced = [len(best_s[i]) for i in range(len(need_check))]
    ids = [dix_org[x[0]] for x in best]
    for i,x in enumerate(matched_org):
       # id_list = []
        if dix_mult[x] != 'unique':
            if x in list(dix_city_ror.keys()):
                match_found0 = False
                match_found = False

                for city in dix_city_ror[x]:
                    if city[0] in light_aff:
                        if city[0] not in x: 
                            ids[i] = city[1]
                            
                            match_found0 = True
                            match_found = True
                            break
                if not match_found:
                    for city in dix_city_ror[x]:
                        if city[0] in   light_aff and city[0] not in x:
                            ids[i] = city[1]
                            match_found0 = True
                            print('ok')
                            break  
                    
                if not match_found:
                    match_found2 = False
                    match_found3 = False

                    for country in dix_country_ror[x]:
                        if country[0] == 'united states' and (country[0] in light_aff or 'usa'  in light_aff):
                            ids[i] = country[1]
                            match_found2 = True
                            match_found3 = True
                            break
                        
                        if country[0] == 'united kingdom' and (country[0] in light_aff or 'uk'  in light_aff):
                            ids[i] = country[1]
                            match_found2 = True
                            match_found3 = True
                            break

                        elif country[0] in light_aff:

                            if country[0] not in x:
                                ids[i] = country[1]
                                match_found2 = True
                                match_found3 = True
                                break

                    if not match_found3:
                        for country in dix_country_ror[x]:
                            if country[0] in light_aff and country[0] in x:
                                ids[i] = country[1]
                                match_found2 = True
                                break  
                        
                
                
            

    results = [[x[0],x[1], ids[i]] for i,x in enumerate(best)]

    return  results #[[result[to_check[i]] for i in ready] + [to_check[2]], best[0]]