set mapred.job.queue.name=analytics;

drop database if exists TARGET cascade;
create database if not exists TARGET;

create table if not exists TARGET.result stored as parquet as
select distinct * from (
    select r.*
    from SOURCE.result r
    join SOURCE.result_projects rp on rp.id=r.id
    join SOURCE.project p on p.id=rp.project
    join SOURCE.funder irf on irf.name=p.funder and irf.country='IE'
    union all
    select r.*
    from SOURCE.result r
    join SOURCE.result_organization ro on ro.id=r.id
    join SOURCE.organization o on o.id=ro.organization and o.country='IE'
    union all
    select r.*
    from SOURCE.result r
    join SOURCE.result_pids pid on pid.id=r.id
    join stats_ext.transformative_facts tf on tf.doi=pid.pid
) foo;

compute stats TARGET.result;