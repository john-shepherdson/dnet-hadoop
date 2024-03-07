DROP VIEW IF EXISTS ${hiveDbName}.result;

CREATE VIEW IF NOT EXISTS ${hiveDbName}.result as
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, metaresourcetype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount, eoscifguidelines from ${hiveDbName}.publication p
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, metaresourcetype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount, eoscifguidelines from ${hiveDbName}.dataset d
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, metaresourcetype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount, eoscifguidelines from ${hiveDbName}.software s
    union all
    select id, originalid, dateofcollection, title, publisher, bestaccessright, datainfo, collectedfrom, pid, author, resulttype, metaresourcetype, language, country, subject, description, dateofacceptance, relevantdate, embargoenddate, resourcetype, context, externalreference, instance, measures, processingchargeamount, eoscifguidelines from ${hiveDbName}.otherresearchproduct o;
