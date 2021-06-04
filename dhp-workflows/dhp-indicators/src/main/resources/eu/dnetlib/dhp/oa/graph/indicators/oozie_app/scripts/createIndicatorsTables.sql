create table TARGET.funders_publications stored as parquet as
select f.id as id, count(pr.result) as total_pubs from SOURCE.funder f 
join SOURCE.project p on f.name=p.funder 
join SOURCE.project_results_publication pr on pr.project_results=p.id group by f.id, f.name;


compute stats TARGET.funders_publications;