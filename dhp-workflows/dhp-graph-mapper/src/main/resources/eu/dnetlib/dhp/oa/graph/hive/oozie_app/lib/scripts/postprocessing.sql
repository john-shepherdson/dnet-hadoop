DROP VIEW IF EXISTS ${hiveDbName}.result;

CREATE VIEW IF NOT EXISTS ${hiveDbName}.result as
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount from ${hiveDbName}.publication p
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount from ${hiveDbName}.dataset d
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount from ${hiveDbName}.software s
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount from ${hiveDbName}.otherresearchproduct o;

ANALYZE TABLE ${hiveDbName}.datasource COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.organization COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.project COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.publication COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.dataset COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.otherresearchproduct COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.software COMPUTE STATISTICS;
ANALYZE TABLE ${hiveDbName}.relation COMPUTE STATISTICS;