create table ${observatory_db_name}.result_cc_licence stored as parquet as
select r.id, coalesce(rln.count, 0) > 0 as cc_licence
from ${stats_db_name}.result r
         left outer join (
    select rl.id, sum(case when rl.type like 'CC%' then 1 else 0 end) as count
    from ${stats_db_name}.result_licenses rl
    group by rl.id
) rln on rln.id=r.id;


create table ${observatory_db_name}.result_affiliated_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, c.code, c.name;


create table ${observatory_db_name}.result_affiliated_year stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    r.year
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, r.year;


create table ${observatory_db_name}.result_affiliated_year_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    r.year, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, r.year, c.code, c.name;


create table ${observatory_db_name}.result_affiliated_datasource stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    d.name as dname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_datasources rd on rd.id=r.id
         left outer join ${stats_db_name}.datasource d on d.id=rd.datasource
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, d.name;

create table ${observatory_db_name}.result_affiliated_datasource_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    d.name as dname, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_datasources rd on rd.id=r.id
         left outer join ${stats_db_name}.datasource d on d.id=rd.datasource
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, d.name, c.code, c.name;

create table ${observatory_db_name}.result_affiliated_organization stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    o.name as oname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, o.name;

create table ${observatory_db_name}.result_affiliated_organization_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    o.name as oname, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, o.name, c.code, c.name;

create table ${observatory_db_name}.result_affiliated_funder stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    p.funder as pfunder
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         join ${stats_db_name}.result_projects rp on rp.id=r.id
         join ${stats_db_name}.project p on p.id=rp.project
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, p.funder;

create table ${observatory_db_name}.result_affiliated_funder_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    p.funder as pfunder, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_organization ro on ro.id=r.id
         join ${stats_db_name}.organization o on o.id=ro.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         join ${stats_db_name}.result_projects rp on rp.id=r.id
         join ${stats_db_name}.project p on p.id=rp.project
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, p.funder, c.code, c.name;

create table ${observatory_db_name}.result_deposited_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, c.code, c.name;

create table ${observatory_db_name}.result_deposited_year stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    r.year
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, r.year;


create table ${observatory_db_name}.result_deposited_year_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    r.year, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, r.year, c.code, c.name;

create table ${observatory_db_name}.result_deposited_datasource stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    d.name as dname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, d.name;

create table ${observatory_db_name}.result_deposited_datasource_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    d.name as dname, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, d.name, c.code, c.name;

create table ${observatory_db_name}.result_deposited_organization stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    o.name as oname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, o.name;

create table ${observatory_db_name}.result_deposited_organization_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    o.name as oname, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, o.name, c.code, c.name;

create table ${observatory_db_name}.result_deposited_funder stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    p.funder as pfunder
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         join ${stats_db_name}.result_projects rp on rp.id=r.id
         join ${stats_db_name}.project p on p.id=rp.project
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, p.funder;

create table ${observatory_db_name}.result_deposited_funder_country stored as parquet as
select
    count(distinct r.id) as total,
    r.green,
    r.gold,
    case when rl.type is not null then true else false end as licence,
    case when pids.pid is not null then true else false end as pid,
    case when r.access_mode in ('Open Access', 'Open Source') then true else false end as oa,
    r.peer_reviewed,
    rln.cc_licence,
    r.abstract as abstract,
    r.authors > 1 as multiple_authors,
    rpc.count > 1 as multiple_projects,
    rfc.count > 1 as multiple_funders,
    r.type,
    p.funder as pfunder, c.code as ccode, c.name as cname
from ${stats_db_name}.result r
         join ${stats_db_name}.result_datasources rd on rd.id=r.id
         join ${stats_db_name}.datasource d on d.id=rd.datasource and d.type in ('Institutional Repository','Data Repository', 'Repository', 'Publication Repository')
         join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
         join ${stats_db_name}.organization o on o.id=dor.organization
         join ${stats_db_name}.country c on c.code=o.country and c.continent_name='Europe'
         join ${stats_db_name}.result_projects rp on rp.id=r.id
         join ${stats_db_name}.project p on p.id=rp.project
         left outer join ${stats_db_name}.result_licenses rl on rl.id=r.id
         left outer join ${stats_db_name}.result_pids pids on pids.id=r.id
         left outer join ${observatory_db_name}.result_cc_licence rln on rln.id=r.id
         left outer join ${stats_db_name}.result_projectcount rpc on rpc.id=r.id
         left outer join ${stats_db_name}.result_fundercount rfc on rfc.id=r.id
group by r.green, r.gold, case when rl.type is not null then true else false end, case when pids.pid is not null then true else false end,
         case when r.access_mode in ('Open Access', 'Open Source') then true else false end, r.peer_reviewed, r.type, abstract,
         cc_licence, r.authors > 1, rpc.count > 1, rfc.count > 1, p.funder, c.code, c.name;
