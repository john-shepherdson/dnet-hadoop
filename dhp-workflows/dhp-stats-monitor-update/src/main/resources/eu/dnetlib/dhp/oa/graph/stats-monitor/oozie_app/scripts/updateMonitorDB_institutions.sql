DROP TABLE IF EXISTS TARGET.result_new;

create table TARGET.result_new as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_organization ro where ro.id=r.id and ro.organization in (
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	--Iscte - Instituto Universitário de Lisboa
             'openorgs____::ab4ac74c35fa5dada770cf08e5110fab'	-- Universidade Católica Portuguesa
        ) )) foo;

INSERT INTO TARGET.result select * from TARGET.result_new;
ANALYZE TABLE TARGET.result_new COMPUTE STATISTICS;

