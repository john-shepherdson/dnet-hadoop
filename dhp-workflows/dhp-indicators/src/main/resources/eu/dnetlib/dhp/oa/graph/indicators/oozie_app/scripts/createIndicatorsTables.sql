create table TARGET.indi_pub_green_oa stored as parquet as
select distinct p.id, coalesce(green_oa, 0) as green_oa 
from SOURCE.publication p 
left outer join ( 
select p.id, 1 as green_oa 
from SOURCE.publication p 
join SOURCE.result_instance ri on ri.id = p.id 
join SOURCE.datasource on datasource.id = ri.hostedby  
where SOURCE.datasource.type like '%Repository%'  
and (ri.accessright = 'Open Access'  
or ri.accessright = 'Embargo')) tmp 
on p.id= tmp.id;

create table TARGET.indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from SOURCE.publication p
left outer join (
select p.id, 1 as grey_lit 
from SOURCE.publication p
join SOURCE.result_classifications rt on rt.id = p.id
where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object') and 
not exists (select 1 from result_classifications rc where type ='Other literature type' and rc.id=p.id)) tmp on p.id=tmp.id;

create table TARGET.indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref 
from SOURCE.publication p
left outer join
(select ri.id, 1 as doi_from_crossref from SOURCE.result_instance ri
join SOURCE.datasource d on d.id = ri.collectedfrom
where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp
on tmp.id=p.id;

create table TARGET.indi_pub_gold_oa stored as parquet as
select distinct p.id, coalesce(gold_oa, 0) as gold_oa
from SOURCE.publication p 
left outer join ( 
select p.id, 1 as gold_oa
from SOURCE.publication p 
join SOURCE.result_instance ri on ri.id = p.id 
join SOURCE.datasource on datasource.id = ri.hostedby  
where SOURCE.datasource.id like '%doajarticles%') tmp 
on p.id= tmp.id;

compute stats TARGET.indi_pub_green_oa;
compute stats TARGET.indi_pub_grey_lit;
compute stats TARGET.indi_pub_doi_from_crossref;
compute stats TARGET.indi_pub_gold_oa;