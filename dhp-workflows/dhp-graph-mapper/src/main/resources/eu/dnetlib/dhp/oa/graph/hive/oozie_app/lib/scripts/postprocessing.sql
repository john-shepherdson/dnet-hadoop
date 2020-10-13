DROP VIEW IF EXISTS ${hiveDbName}.result;

CREATE VIEW IF NOT EXISTS ${hiveDbName}.result as
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance from ${hiveDbName}.publication p
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance from ${hiveDbName}.dataset d
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance from ${hiveDbName}.software s
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance from ${hiveDbName}.otherresearchproduct o;
