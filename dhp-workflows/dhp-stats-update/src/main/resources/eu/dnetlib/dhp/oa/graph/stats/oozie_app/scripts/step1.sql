-- DROP database if EXISTS ${hive_db_name} cascade;
-- CREATE database ${hive_db_name};
-- 
-- CREATE TABLE ${hive_db_name}.Persons ( 
-- PersonID int, 
-- LastName varchar(255));
-- 
-- INSERT INTO ${hive_db_name}.Persons VALUES (1, "test_db_spyros_rec_111"); 

DROP database IF EXISTS ${stats_db_name} CASCADE;
CREATE database ${stats_db_name};
