from functions_cluster import *

def create_df_algorithm(raw_aff_string):


    aff_no_symbols_d =  substrings_dict(clean_string(remove_outer_parentheses(remove_leading_numbers(raw_aff_string))))

    dict_ = list(aff_no_symbols_d.values())

    i = 0


    while i < len(dict_) - 1:
        if is_contained('progr', dict_[i]) and is_contained('dep', dict_[i+1]):
            dict_.pop(i)

            
        elif (is_contained('assistant', dict_[i]) or is_contained('researcher', dict_[i]) or is_contained('phd', dict_[i]) or is_contained('student', dict_[i]) or is_contained('section', dict_[i]) or is_contained('prof', dict_[i]) or is_contained('director', dict_[i])) and (not is_contained('school', dict_[i+1]) or is_contained('univ', dict_[i+1]) or is_contained('inst', dict_[i+1]) or is_contained('lab', dict_[i+1]) or is_contained('fac', dict_[i+1])):
            dict_.pop(i)

        elif (is_contained('engineer', dict_[i]) or is_contained('progr', dict_[i]) or is_contained('unit', dict_[i]) or is_contained('lab', dict_[i]) or is_contained('dep', dict_[i]) or  is_contained('school', dict_[i])  or is_contained('inst', dict_[i]) #or is_contained('hosp', dict_[i]) 
            or is_contained('fac', dict_[i])) and is_contained('univ', dict_[i+1]):
            if not is_contained('univ', dict_[i]):
                dict_.pop(i)

        elif is_contained('lab', dict_[i]) and (is_contained('colege', dict_[i+1]) or is_contained('inst', dict_[i+1]) or is_contained('dep', dict_[i+1]) or is_contained('school', dict_[i+1])):
            if not is_contained('univ', dict_[i]):
                dict_.pop(i)

        elif is_contained('dep', dict_[i]) and (is_contained('tech', dict_[i+1]) or is_contained('colege', dict_[i+1]) or is_contained('inst', dict_[i+1]) or  is_contained('hosp', dict_[i+1]) or  is_contained('school', dict_[i+1]) or  is_contained('fac', dict_[i+1])):
            if not is_contained('univ', dict_[i]):
                dict_.pop(i)

        elif is_contained('inst',dict_[i]) and (is_contained('school', dict_[i+1]) or is_contained('dep', dict_[i+1]) or is_contained('acad', dict_[i+1]) or is_contained('hosp', dict_[i+1]) or is_contained('clin', dict_[i+1]) or is_contained('klin', dict_[i+1])  or is_contained('fak', dict_[i+1]) or is_contained('fac', dict_[i+1]) or is_contained('cent', dict_[i+1]) or is_contained('div', dict_[i+1])):
            if not is_contained('univ', dict_[i]):
                dict_.pop(i)

        elif is_contained('school',dict_[i]) and is_contained('colege', dict_[i+1]):
            if not is_contained('univ', dict_[i]):
                dict_.pop(i)
        else:
            i += 1

    light_aff = (', '.join((dict_)))

    for x in dict_:
        if x in city_names+remove_list:
            dict_.remove(x)
    

    dict_ = [shorten_keywords_spark([x])[0] for x in dict_] 

    keywords= []
    def valueToCategory(value):
        flag = 0

        for k in categ_dicts:
            if k in value: 
                flag = 1
        return flag
    
    aff_list = [{"index": i, "keywords": dict_[i], "category": valueToCategory(dict_[i])} for i in range(len(dict_))]

    filtered_list = [entry for entry in aff_list if entry.get("category") == 1]

    return   [light_aff, filtered_list]