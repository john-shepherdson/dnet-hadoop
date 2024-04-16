----------------------------------------------------------------
----------------------------------------------------------------
-- Post processing - Updates on main tables
----------------------------------------------------------------
----------------------------------------------------------------

--Datasource temporary table updates
UPDATE ${stats_db_name}.datasource_tmp
SET harvested='true'
WHERE datasource_tmp.id IN (SELECT DISTINCT d.id
                            FROM ${stats_db_name}.datasource_tmp d,
                                 ${stats_db_name}.result_datasources rd
                            WHERE d.id = rd.datasource); /*EOS*/

-- Project temporary table update and final project table creation with final updates that can not be applied to ORC tables
UPDATE ${stats_db_name}.project_tmp
SET haspubs='yes'
WHERE project_tmp.id IN (SELECT pr.id
                         FROM ${stats_db_name}.project_results pr,
                              ${stats_db_name}.result r
                         WHERE pr.result = r.id
                           AND r.type = 'publication'); /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project stored as parquet as
SELECT p.id,
       p.acronym,
       p.title,
       p.funder,
       p.funding_lvl0,
       p.funding_lvl1,
       p.funding_lvl2,
       p.ec39,
       p.type,
       p.startdate,
       p.enddate,
       p.start_year,
       p.end_year,
       p.duration,
       CASE WHEN prr1.id IS NULL THEN 'no' ELSE 'yes' END            AS haspubs,
       CASE WHEN prr1.id IS NULL THEN 0 ELSE prr1.np END             AS numpubs,
       CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.daysForlastPub END AS daysforlastpub,
       CASE WHEN prr2.id IS NULL THEN 0 ELSE prr2.dp END             AS delayedpubs,
       p.callidentifier,
       p.code,
       p.totalcost,
       p.fundedamount,
       p.currency
FROM ${stats_db_name}.project_tmp p
         LEFT JOIN (SELECT pr.id, count(distinct pr.result) AS np
                    FROM ${stats_db_name}.project_results pr
                             INNER JOIN ${stats_db_name}.result r ON pr.result = r.id
                    WHERE r.type = 'publication'
                    GROUP BY pr.id) AS prr1 on prr1.id = p.id
         LEFT JOIN (SELECT pp.id,
                           max(datediff(to_date(r.date), to_date(pp.enddate))) AS daysForlastPub,
                           count(distinct r.id)                                AS dp
                    FROM ${stats_db_name}.project_tmp pp,
                         ${stats_db_name}.project_results pr,
                         ${stats_db_name}.result r
                    WHERE pp.id = pr.id
                      AND pr.result = r.id
                      AND r.type = 'publication'
                      AND datediff(to_date(r.date), to_date(pp.enddate)) > 0
                    GROUP BY pp.id) AS prr2
                   ON prr2.id = p.id; /*EOS*/

UPDATE ${stats_db_name}.publication_tmp
SET delayed = 'yes'
WHERE publication_tmp.id IN (SELECT distinct r.id
                             FROM ${stats_db_name}.result r,
                                  ${stats_db_name}.project_results pr,
                                  ${stats_db_name}.project_tmp p
                             WHERE r.id = pr.result
                               AND pr.id = p.id
                               AND to_date(r.date) - to_date(p.enddate) > 0); /*EOS*/

UPDATE ${stats_db_name}.dataset_tmp
SET delayed = 'yes'
WHERE dataset_tmp.id IN (SELECT distinct r.id
                         FROM ${stats_db_name}.result r,
                              ${stats_db_name}.project_results pr,
                              ${stats_db_name}.project_tmp p
                         WHERE r.id = pr.result
                           AND pr.id = p.id
                           AND to_date(r.date) - to_date(p.enddate) > 0); /*EOS*/

UPDATE ${stats_db_name}.software_tmp
SET delayed = 'yes'
WHERE software_tmp.id IN (SELECT distinct r.id
                          FROM ${stats_db_name}.result r,
                               ${stats_db_name}.project_results pr,
                               ${stats_db_name}.project_tmp p
                          WHERE r.id = pr.result
                            AND pr.id = p.id
                            AND to_date(r.date) - to_date(p.enddate) > 0); /*EOS*/

UPDATE ${stats_db_name}.otherresearchproduct_tmp
SET delayed = 'yes'
WHERE otherresearchproduct_tmp.id IN (SELECT distinct r.id
                                      FROM ${stats_db_name}.result r,
                                           ${stats_db_name}.project_results pr,
                                           ${stats_db_name}.project_tmp p
                                      WHERE r.id = pr.result
                                        AND pr.id = p.id
                                        AND to_date(r.date) - to_date(p.enddate) > 0); /*EOS*/

CREATE OR REPLACE VIEW ${stats_db_name}.project_results_publication AS
SELECT result_projects.id          AS result,
       result_projects.project     AS project_results,
       result.date                 as resultdate,
       project.enddate             as projectenddate,
       result_projects.daysfromend AS daysfromend
FROM ${stats_db_name}.result_projects,
     ${stats_db_name}.result,
     ${stats_db_name}.project
WHERE result_projects.id = result.id
  AND result.type = 'publication'
  AND project.id = result_projects.project; /*EOS*/