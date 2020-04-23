DROP VIEW IF EXISTS ${hiveDbName}.result;

CREATE VIEW IF NOT EXISTS result as
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hiveDbName}.publication p
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hiveDbName}.dataset d
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hiveDbName}.software s
    union all
    select id, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, embargoenddate, resourcetype, context, instance from ${hiveDbName}.otherresearchproduct o;
