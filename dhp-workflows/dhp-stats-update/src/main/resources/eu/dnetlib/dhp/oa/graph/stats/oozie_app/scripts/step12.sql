----------------------------
-- Post processing - Updates
----------------------------

--Datasource temporary table updates
UPDATE ${hive_db_name}.datasource_tmp set harvested ='true' WHERE datasource_tmp.id IN (SELECT DISTINCT d.id FROM ${hive_db_name}.datasource_tmp d, ${hive_db_name}.result_datasources rd where d.id=rd.datasource);

-- Project temporary table update and final project table creation with final updates that can not be applied to ORC tables
-- UPDATE ${hive_db_name}.project_tmp SET haspubs='yes' WHERE project_tmp.id IN (SELECT pr.id FROM ${hive_db_name}.project_results pr, ${hive_db_name}.result r WHERE pr.result=r.id AND r.type='publication');


CREATE TABLE ${hive_db_name}.project stored as parquet as
SELECT p.id , p.acronym, p.title, p.funder, p.funding_lvl0, p.funding_lvl1, p.funding_lvl2, p.ec39, p.type, p.startdate, p.enddate, p.start_year, p.end_year, p.duration,
CASE WHEN prr1.id IS NULL THEN 'no' ELSE 'yes' END as haspubs,
CASE WHEN prr1.id IS NULL THEN 0 ELSE prr1.np END as numpubs,
CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.daysForlastPub END as daysforlastpub,
CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.dp END as delayedpubs,
p.callidentifier, p.code
FROM ${hive_db_name}.project_tmp p
LEFT JOIN (SELECT pr.id, count(distinct pr.result) AS np
        FROM ${hive_db_name}.project_results pr INNER JOIN ${hive_db_name}.result r ON pr.result=r.id
        WHERE r.type='publication'
        GROUP BY pr.id) AS prr1 on prr1.id = p.id
LEFT JOIN (SELECT pp.id, max(datediff(to_date(r.date), to_date(pp.enddate)) ) as daysForlastPub , count(distinct r.id) as dp
        FROM ${hive_db_name}.project_tmp pp, ${hive_db_name}.project_results pr, ${hive_db_name}.result r
        WHERE pp.id=pr.id AND pr.result=r.id AND r.type='publication' AND datediff(to_date(r.date), to_date(pp.enddate)) > 0
        GROUP BY pp.id) AS prr2
        on prr2.id = p.id;


-- Publication temporary table updates
UPDATE ${hive_db_name}.publication_tmp SET delayed = 'yes' WHERE publication_tmp.id IN (SELECT distinct r.id FROM ${hive_db_name}.result r, ${hive_db_name}.project_results pr, ${hive_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);

-- Dataset temporary table updates
UPDATE ${hive_db_name}.dataset_tmp SET delayed = 'yes' WHERE dataset_tmp.id IN (SELECT distinct r.id FROM ${hive_db_name}.result r, ${hive_db_name}.project_results pr, ${hive_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);

-- Software temporary table updates
UPDATE ${hive_db_name}.software_tmp SET delayed = 'yes' WHERE software_tmp.id IN (SELECT distinct r.id FROM ${hive_db_name}.result r, ${hive_db_name}.project_results pr, ${hive_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);

-- Oherresearchproduct temporary table updates
UPDATE ${hive_db_name}.otherresearchproduct_tmp SET delayed = 'yes' WHERE otherresearchproduct_tmp.id IN (SELECT distinct r.id FROM ${hive_db_name}.result r, ${hive_db_name}.project_results pr, ${hive_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);


CREATE OR REPLACE VIEW ${hive_db_name}.project_results_publication AS SELECT result_projects.id AS result, result_projects.project AS project_results, result.date as resultdate, project.enddate as projectenddate, result_projects.daysfromend as daysfromend FROM  ${hive_db_name}.result_projects, ${hive_db_name}.result, ${hive_db_name}.project WHERE result_projects.id=result.id and result.type='publication' and project.id=result_projects.project;
