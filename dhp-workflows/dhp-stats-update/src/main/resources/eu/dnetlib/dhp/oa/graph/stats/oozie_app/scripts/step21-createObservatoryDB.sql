create table TARGET.result_affiliated_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
    left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, c.code, c.name;

create table TARGET.result_affiliated_year stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa, r.peer_reviewed,
       coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, r.year
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, r.year;

create table TARGET.result_affiliated_year_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, r.year, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, r.year, c.code, c.name;

create table TARGET.result_affiliated_datasource stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa, r.peer_reviewed,
       coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, d.name as dname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_datasources rd on rd.id=r.id
left outer join SOURCE.datasource d on d.id=rd.datasource
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, d.name;

create table TARGET.result_affiliated_datasource_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, d.name as dname, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_datasources rd on rd.id=r.id
left outer join SOURCE.datasource d on d.id=rd.datasource
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, d.name, c.code, c.name;

create table TARGET.result_affiliated_organization stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, o.name as oname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, o.name;

create table TARGET.result_affiliated_organization_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, o.name as oname, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, o.name, c.code, c.name;

create table TARGET.result_affiliated_funder stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa, r.peer_reviewed,
       coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, p.funder as pfunder
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
join SOURCE.result_projects rp on rp.id=r.id
join SOURCE.project p on p.id=rp.project
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, p.funder;

create table TARGET.result_affiliated_funder_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, p.funder as pfunder, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_organization ro on ro.id=r.id
join SOURCE.organization o on o.id=ro.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
join SOURCE.result_projects rp on rp.id=r.id
join SOURCE.project p on p.id=rp.project
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, p.funder, c.code, c.name;

create table TARGET.result_deposited_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, c.code, c.name;

create table TARGET.result_deposited_year stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa, r.peer_reviewed,
       coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, r.year
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, r.year;

create table TARGET.result_deposited_year_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, r.year, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, r.year, c.code, c.name;

create table TARGET.result_deposited_datasource stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, d.name as dname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, d.name;

create table TARGET.result_deposited_datasource_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, d.name as dname, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, d.name, c.code, c.name;

create table TARGET.result_deposited_organization stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa, r.peer_reviewed,
       coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, o.name as oname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, o.name;

create table TARGET.result_deposited_organization_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, o.name as oname, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, o.name, c.code, c.name;

create table TARGET.result_deposited_funder stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, p.funder as pfunder
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
join SOURCE.result_projects rp on rp.id=r.id
join SOURCE.project p on p.id=rp.project
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, p.funder;

create table TARGET.result_deposited_funder_country stored as parquet as
select count(distinct r.id) as total, r.green, r.gold, case when rl.type is not null then true else false end as licence,
  case when pids.pid is not null then true else false end as pid, case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
  r.peer_reviewed, coalesce(rln.count, 0) > 0 as cc_licence, r.abstract as abstract, r.type, p.funder as pfunder, c.code as ccode, c.name as cname
from SOURCE.result r
join SOURCE.result_datasources rd on rd.id=r.id
join SOURCE.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
join SOURCE.datasource_organizations dor on dor.id=d.id
join SOURCE.organization o on o.id=dor.organization
join SOURCE.country c on c.code=o.country and c.continent_name='Europe'
join SOURCE.result_projects rp on rp.id=r.id
join SOURCE.project p on p.id=rp.project
left outer join SOURCE.result_licenses rl on rl.id=r.id
left outer join SOURCE.result_pids pids on pids.id=r.id
left outer join (
    select rl.id, sum(case when lower(rln.normalized) like 'cc-%' then 1 else 0 end) as count
    from SOURCE.result_licenses rl
        left outer join SOURCE.licenses_normalized rln on rl.type=rln.license
    group by rl.id
) rln on rln.id=r.id
group by r.green, r.gold, licence, pid, oa, r.peer_reviewed, r.type, cc_licence, abstract, p.funder, c.code, c.name;

compute stats TARGET.result_affiliated_country;
compute stats TARGET.result_affiliated_year;
compute stats TARGET.result_affiliated_year_country;
compute stats TARGET.result_affiliated_datasource;
compute stats TARGET.result_affiliated_datasource_country;
compute stats TARGET.result_affiliated_organization;
compute stats TARGET.result_affiliated_organization_country;
compute stats TARGET.result_affiliated_funder;
compute stats TARGET.result_affiliated_funder_country;
compute stats TARGET.result_deposited_country;
compute stats TARGET.result_deposited_year;
compute stats TARGET.result_deposited_year_country;
compute stats TARGET.result_deposited_datasource;
compute stats TARGET.result_deposited_datasource_country;
compute stats TARGET.result_deposited_organization;
compute stats TARGET.result_deposited_organization_country;
compute stats TARGET.result_deposited_funder;
compute stats TARGET.result_deposited_funder_country;