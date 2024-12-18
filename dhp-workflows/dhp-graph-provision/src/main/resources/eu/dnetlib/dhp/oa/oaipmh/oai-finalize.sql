BEGIN; 
	
DELETE FROM oai_data; 

INSERT INTO oai_data(id, body, date, sets) SELECT 
	id, 
	body, 
	date::timestamp, 
	sets 
FROM temp_oai_data;

COMMIT;
