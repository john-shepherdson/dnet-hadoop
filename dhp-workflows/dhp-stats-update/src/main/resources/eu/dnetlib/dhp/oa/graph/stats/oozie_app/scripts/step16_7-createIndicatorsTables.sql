create table indi_pub_green_oa stored as parquet as
select distinct p.id, coalesce(green_oa, 0) as green_oa 
from publication p
left outer join ( 
select p.id, 1 as green_oa 
from publication p
join result_instance ri on ri.id = p.id
join datasource on datasource.id = ri.hostedby
where datasource.type like '%Repository%'
and (ri.accessright = 'Open Access'  
or ri.accessright = 'Embargo')) tmp 
on p.id= tmp.id;

create table indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from publication p
left outer join (
select p.id, 1 as grey_lit 
from publication p
join result_classifications rt on rt.id = p.id
where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object') and 
not exists (select 1 from result_classifications rc where type ='Other literature type' and rc.id=p.id)) tmp on p.id=tmp.id;

create table indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref 
from publication p
left outer join
(select ri.id, 1 as doi_from_crossref from result_instance ri
join datasource d on d.id = ri.collectedfrom
where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp
on tmp.id=p.id;

create table indi_pub_gold_oa stored as parquet as
select distinct p.id, coalesce(gold_oa, 0) as gold_oa
from publication p
left outer join ( 
select p.id, 1 as gold_oa
from publication p
join result_instance ri on ri.id = p.id
join datasource on datasource.id = ri.hostedby
where datasource.id like '%doajarticles%') tmp
on p.id= tmp.id;