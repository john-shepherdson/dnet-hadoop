set mapred.job.queue.name=analytics;

drop database if exists TARGET cascade;
create database if not exists TARGET;

create table TARGET.result stored as parquet as
    select distinct * from (
        select * from SOURCE.result r where exists
            (select 1
             from SOURCE.result_concepts rc
             join SOURCE.concept conc on conc.id=rc.concept
             join SOURCE.category cat on cat.id=conc.category
             join SOURCE.context cont on cont.id=cat.context
--             join SOURCE.result
             where rc.id=r.id and conc.category not in (CONTEXTS))
)  foo;