SET spark.sql.parquet.writer.version = v1;

DROP DATABASE IF EXISTS ${usagestats_db} CASCADE; /*EOS*/
CREATE DATABASE IF NOT EXISTS ${usagestats_db}; /*EOS*/