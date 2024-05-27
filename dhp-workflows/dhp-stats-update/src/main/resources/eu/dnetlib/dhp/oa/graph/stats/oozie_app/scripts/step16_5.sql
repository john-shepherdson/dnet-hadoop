set mapred.job.queue.name=analytics; /*EOS*/

-- replace the creation of the result view to include the boolean fields from the previous tables (green, gold,
-- peer reviewed)
drop table if exists ${stats_db_name}.result_tmp; /*EOS*/

CREATE TABLE ${stats_db_name}.result_tmp (
    id STRING,
    title STRING,
    publisher STRING,
    journal STRING,
    `date` STRING,
    `year` INT,
    bestlicence STRING,
    access_mode STRING,
    embargo_end_date STRING,
    delayed BOOLEAN,
    authors INT,
    source STRING,
    abstract BOOLEAN,
    type STRING ,
    peer_reviewed BOOLEAN,
    green BOOLEAN,
    gold BOOLEAN)
clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true'); /*EOS*/

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.publication r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.dataset r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.software r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.otherresearchproduct r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/

drop table if exists ${stats_db_name}.result; /*EOS*/
drop view if exists ${stats_db_name}.result; /*EOS*/
create table ${stats_db_name}.result stored as parquet as select * from ${stats_db_name}.result_tmp; /*EOS*/
drop table ${stats_db_name}.result_tmp; /*EOS*/