set mapred.job.queue.name=analytics; /*EOS*/

--------------------------------------------------------------
--------------------------------------------------------------
-- Stats database creation
--------------------------------------------------------------
--------------------------------------------------------------

DROP database IF EXISTS ${stats_db_name} CASCADE; /*EOS*/
CREATE database ${stats_db_name}; /*EOS*/
