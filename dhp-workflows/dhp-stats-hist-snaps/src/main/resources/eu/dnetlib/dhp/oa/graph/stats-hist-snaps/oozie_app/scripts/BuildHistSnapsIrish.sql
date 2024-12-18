INSERT INTO ${hist_db_name}.historical_snapshots_fos_irish_tmp
SELECT * FROM ${hist_db_name_prev}.historical_snapshots_irish_fos;

INSERT INTO ${hist_db_name}.historical_snapshots_fos_irish_tmp
select
    cast(${hist_date} as STRING),
    count(distinct r.id),
    r.type,
    rf.lvl1,
    rf.lvl2,
    pf.publicly_funded,
    r.access_mode,
    r.gold,
    r.green,
    coalesce(gl.green_with_license,0),
    h.is_hybrid,
    b.is_bronze_oa,
    d.in_diamond_journal,
    t.is_transformative,
    pr.refereed
from ${stats_irish_db_name}.result r
         left outer join ${stats_irish_db_name}.result_fos rf on rf.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_publicly_funded pf on pf.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_green_with_license gl on gl.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_bronze_oa b on b.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_diamond d on d.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_in_transformative t on t.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_hybrid h on h.id=r.id
         left outer join ${stats_irish_db_name}.result_refereed pr on pr.id=r.id
group by r.green, r.gold, r.access_mode, r.type, rf.lvl1,rf.lvl2, pf.publicly_funded,r.green, gl.green_with_license,b.is_bronze_oa,d.in_diamond_journal,t.is_transformative,h.is_hybrid,pr.refereed;

drop table if exists ${hist_db_name}.historical_snapshots_irish_fos purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_irish_fos STORED AS PARQUET AS
SELECT * FROM ${hist_db_name}.historical_snapshots_fos_irish_tmp;

drop table if exists ${monitor_irish_db_name}.historical_snapshots_irish_fos purge;

create table ${monitor_irish_db_name}.historical_snapshots_irish_fos stored as parquet
as select * from ${hist_db_name}.historical_snapshots_irish_fos;

drop table ${hist_db_name}.historical_snapshots_fos_irish_tmp purge;

INSERT INTO ${hist_db_name}.historical_snapshots_irish_tmp
SELECT * FROM ${hist_db_name_prev}.historical_snapshots_irish;

INSERT INTO ${hist_db_name}.historical_snapshots_irish_tmp
select
    cast(${hist_date} as STRING),
    count(distinct r.id),
    r.type,
    pf.publicly_funded,
    r.access_mode,
    r.gold,
    r.green,
    coalesce(gl.green_with_license,0),
    h.is_hybrid,
    b.is_bronze_oa,
    d.in_diamond_journal,
    t.is_transformative,
    pr.refereed
from ${stats_irish_db_name}.result r
         left outer join ${stats_irish_db_name}.indi_pub_publicly_funded pf on pf.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_green_with_license gl on gl.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_bronze_oa b on b.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_diamond d on d.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_in_transformative t on t.id=r.id
         left outer join ${stats_irish_db_name}.indi_pub_hybrid h on h.id=r.id
         left outer join ${stats_irish_db_name}.result_refereed pr on pr.id=r.id
group by r.green, r.gold, r.access_mode, r.type, pf.publicly_funded,r.green, gl.green_with_license,b.is_bronze_oa,d.in_diamond_journal,t.is_transformative,h.is_hybrid,pr.refereed;


drop table if exists ${hist_db_name}.historical_snapshots_irish purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_irish STORED AS PARQUET AS
SELECT * FROM ${hist_db_name}.historical_snapshots_irish_tmp;

drop table if exists ${monitor_irish_db_name}.historical_snapshots_irish purge;

create table ${monitor_irish_db_name}.historical_snapshots_irish stored as parquet
as select * from ${hist_db_name}.historical_snapshots_irish;

drop table ${hist_db_name}.historical_snapshots_irish_tmp purge;


drop table if exists ${monitor_irish_db_name}.historical_snapshots_irish_fos purge;

create table ${monitor_irish_db_name}.historical_snapshots_irish_fos stored as parquet
as select * from ${hist_db_name}.historical_snapshots_irish_fos;

drop table ${hist_db_name}.historical_snapshots_fos_irish_tmp purge;