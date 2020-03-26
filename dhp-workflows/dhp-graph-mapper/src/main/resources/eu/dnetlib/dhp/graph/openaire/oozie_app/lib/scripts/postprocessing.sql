DROP VIEW IF EXISTS ${hive_db_name}.result;

CREATE VIEW IF NOT EXISTS result as
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.publication p
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.dataset d
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.software s
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hive_db_name}.otherresearchproduct o;
