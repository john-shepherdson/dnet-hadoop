drop database if exists TARGET cascade;
create database if not exists TARGET;
set mapred.job.queue.name=analytics; /*EOS*/


create table TARGET.result stored as parquet as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_projects rp join SOURCE.project p on rp.project=p.id where rp.id=r.id)
    ) foo;