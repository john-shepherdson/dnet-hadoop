CREATE view result as
    select id, dateofcollection, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.publication p
    union all
    select id, dateofcollection, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.dataset d
    union all
    select id, dateofcollection, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.software s
    union all
    select id, dateofcollection, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.otherresearchproduct o;
