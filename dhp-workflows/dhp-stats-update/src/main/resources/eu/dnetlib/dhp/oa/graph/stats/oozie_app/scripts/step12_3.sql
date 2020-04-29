DROP TABLE IF EXISTS ${stats_db_name}.project;

CREATE TABLE ${stats_db_name}.project stored as parquet as
SELECT p.id , p.acronym, p.title, p.funder, p.funding_lvl0, p.funding_lvl1, p.funding_lvl2, p.ec39, p.type, p.startdate, p.enddate, p.start_year, p.end_year, p.duration, 
CASE WHEN prr1.id IS NULL THEN 'no' ELSE 'yes' END as haspubs, 
CASE WHEN prr1.id IS NULL THEN 0 ELSE prr1.np END as numpubs, 
CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.daysForlastPub END as daysforlastpub, 
CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.dp END as delayedpubs,
p.callidentifier, p.code
FROM ${stats_db_name}.project_tmp p 
LEFT JOIN (SELECT pr.id, count(distinct pr.result) AS np
        FROM ${stats_db_name}.project_results pr INNER JOIN ${stats_db_name}.result r ON pr.result=r.id 
        WHERE r.type='publication' 
        GROUP BY pr.id) AS prr1 on prr1.id = p.id
LEFT JOIN (SELECT pp.id, max(datediff(to_date(r.date), to_date(pp.enddate)) ) as daysForlastPub , count(distinct r.id) as dp
        FROM ${stats_db_name}.project_tmp pp, ${stats_db_name}.project_results pr, ${stats_db_name}.result r 
        WHERE pp.id=pr.id AND pr.result=r.id AND r.type='publication' AND datediff(to_date(r.date), to_date(pp.enddate)) > 0 
        GROUP BY pp.id) AS prr2
        on prr2.id = p.id;
        