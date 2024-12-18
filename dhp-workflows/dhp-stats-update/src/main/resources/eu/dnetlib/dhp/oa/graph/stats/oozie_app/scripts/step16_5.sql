set mapred.job.queue.name=analytics; /*EOS*/

-- replace the creation of the result view with a table, which will include the boolean fields from the previous tables (green, gold,
-- peer reviewed)

drop view if exists ${stats_db_name}.result; /*EOS*/
drop table if exists ${stats_db_name}.result; /*EOS*/

CREATE TABLE ${stats_db_name}.result stored as parquet as
SELECT /*+ COALESCE(100) */ r.id, r.title, r.publisher, r.journal, r.`date`, DATE_FORMAT(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM (
    (SELECT id, title, p.publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy'), bestlicence, bestlicence, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.publication)
    UNION ALL
    (SELECT id, title, p.publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy'), bestlicence, bestlicence, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.dataset)
    UNION ALL
    (select id, title, p.publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy'), bestlicence, bestlicence, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.software)
    UNION ALL
    (select id, title, p.publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy'), bestlicence, bestlicence, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.otherresearchproduct)
    ) r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/
