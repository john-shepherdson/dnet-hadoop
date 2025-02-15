set mapred.job.queue.name=analytics; /*EOS*/

-- replace the creation of the result view with a table, which will include the boolean fields from the previous tables (green, gold,
-- peer reviewed)

drop view if exists ${stats_db_name}.result; /*EOS*/

CREATE TABLE ${stats_db_name}.result stored as parquet as
SELECT /*+ COALESCE(100) */ r.id, r.title, r.publisher, r.journal, r.`date`, r.year, r.bestlicence, r.access_mode, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM (
    (SELECT id, title, publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy') as year, bestlicence, bestlicence as access_mode, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.publication)
    UNION ALL
    (SELECT id, title, publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy') as year, bestlicence, bestlicence as access_mode, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.dataset)
    UNION ALL
    (select id, title, publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy') as year, bestlicence, bestlicence as access_mode, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.software)
    UNION ALL
    (select id, title, publisher, journal, `date`, DATE_FORMAT(`date`, 'yyyy') as year, bestlicence, bestlicence as access_mode, embargo_end_date, delayed, authors, source, abstract, type
        FROM ${stats_db_name}.otherresearchproduct)
    ) r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id; /*EOS*/

drop table if exists ${stats_db_name}.cross_country_fos; /*EOS*/
create table if not exists ${stats_db_name}.cross_country_fos stored as parquet as
select count(distinct result.id) as total
     ,result.year
     ,result.access_mode
     ,result_refereed.refereed
     ,country.name as country
     ,indi_result_oa_with_license.oa_with_license
     ,indi_result_oa_without_license.oa_without_license
     ,indi_pub_green_with_license.green_with_license
     ,indi_pub_hybrid.is_hybrid
     ,indi_pub_gold_oa.is_gold
     ,indi_pub_green_oa.green_oa
     ,indi_pub_bronze_oa.is_bronze_oa
     ,indi_pub_publicly_funded.publicly_funded
     ,result_fos.lvl1
     ,result_fos.lvl2
from ${stats_db_name}.result
         left outer join ${stats_db_name}.result_refereed on result_refereed.id=result.id
         left outer join ${stats_db_name}.result_country on result_country.id=result.id
         left outer join ${stats_db_name}.indi_result_oa_with_license on indi_result_oa_with_license.id=result.id
         left outer join ${stats_db_name}.indi_result_oa_without_license on indi_result_oa_without_license.id=result.id
         left outer join ${stats_db_name}.indi_pub_green_with_license on indi_pub_green_with_license.id=result.id
         left outer join ${stats_db_name}.indi_pub_hybrid on indi_pub_hybrid.id=result.id
         left outer join ${stats_db_name}.indi_pub_gold_oa on indi_pub_gold_oa.id=result.id
         left outer join ${stats_db_name}.indi_pub_green_oa on indi_pub_green_oa.id=result.id
         left outer join ${stats_db_name}.indi_pub_bronze_oa on indi_pub_bronze_oa.id=result.id
         left outer join ${stats_db_name}.indi_pub_publicly_funded on indi_pub_publicly_funded.id=result.id
         left outer join ${stats_db_name}.result_fos on result_fos.id=result.id
         left outer join ${stats_db_name}.country on country.code=result_country.country

group by result.access_mode
       ,result.year
       ,result_refereed.refereed
       ,country.name
       ,indi_result_oa_with_license.oa_with_license
       ,indi_result_oa_without_license.oa_without_license
       ,indi_pub_green_with_license.green_with_license
       ,indi_pub_hybrid.is_hybrid
       ,indi_pub_gold_oa.is_gold
       ,indi_pub_green_oa.green_oa
       ,indi_pub_bronze_oa.is_bronze_oa
       ,indi_pub_publicly_funded.publicly_funded
       ,result_fos.lvl1
       ,result_fos.lvl2; /*EOS*/
