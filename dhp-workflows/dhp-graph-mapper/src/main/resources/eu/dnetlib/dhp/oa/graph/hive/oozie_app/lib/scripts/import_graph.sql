
CREATE TEMPORARY VIEW datasource USING json OPTIONS ( path "${inputPath}/datasource"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.datasource
    USING parquet
    CLUSTERED BY ( id ) INTO 200 BUCKETS
    AS SELECT * FROM datasource DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW dataset USING json OPTIONS ( path "${inputPath}/dataset"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.dataset
    USING parquet
    CLUSTERED BY ( id ) INTO 4000 BUCKETS
    AS SELECT * FROM dataset DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW organization USING json OPTIONS ( path "${inputPath}/organization"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.organization
    USING parquet
    CLUSTERED BY ( id ) INTO 1000 BUCKETS
    AS SELECT * FROM organization DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW otherresearchproduct USING json OPTIONS ( path "${inputPath}/otherresearchproduct"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.otherresearchproduct
    USING parquet
    CLUSTERED BY ( id ) INTO 8000 BUCKETS
    AS SELECT * FROM otherresearchproduct DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW project USING json OPTIONS ( path "${inputPath}/project"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.project
    USING parquet
    CLUSTERED BY ( id ) INTO 1000 BUCKETS
    AS SELECT * FROM project DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW publication USING json OPTIONS ( path "${inputPath}/publication"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.publication
    USING parquet
    CLUSTERED BY ( id ) INTO 10000 BUCKETS
    AS SELECT * FROM publication DISTRIBUTE BY id;  /*EOS*/

CREATE TEMPORARY VIEW relation USING json OPTIONS ( path "${inputPath}/relation"); /*EOS*/
DROP TABLE IF EXISTS ${hiveDbName}.relation; /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.relation
    USING parquet
    PARTITIONED BY ( relClass )
    AS SELECT * FROM relation DISTRIBUTE BY source,target; /*EOS*/

CREATE TEMPORARY VIEW software USING json OPTIONS ( path "${inputPath}/software"); /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.software
    USING parquet
    CLUSTERED BY ( id ) INTO 1000 BUCKETS
    AS SELECT * FROM software DISTRIBUTE BY source;  /*EOS*/