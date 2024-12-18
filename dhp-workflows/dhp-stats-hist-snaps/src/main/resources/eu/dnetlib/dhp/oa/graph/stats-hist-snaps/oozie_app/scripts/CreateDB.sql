--------------------------------------------------------------
--------------------------------------------------------------
-- Historical Snapshots database creation
--------------------------------------------------------------
--------------------------------------------------------------

DROP database IF EXISTS ${hist_db_name} CASCADE;
CREATE database ${hist_db_name};

drop table if exists ${hist_db_name}.historical_snapshots_fos_tmp purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_fos_tmp
(
    hist_date        STRING,
    total            INT,
    type             STRING,
    lvl1             STRING,
    lvl2             STRING,
    publicly_funded  INT,
    accessrights      STRING,
    gold            INT,
    green          INT,
    green_with_license          INT,
    hybrid           INT,
    bronze         INT,
    diamond             INT,
    transformative  INT,
    peer_reviewed   STRING
)
CLUSTERED BY (hist_date) INTO 100 buckets  stored as orc tblproperties ('transactional' = 'true');

drop table if exists ${hist_db_name}.historical_snapshots_fos_irish_tmp purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_fos_irish_tmp
(
    hist_date        STRING,
    total            INT,
    type             STRING,
    lvl1             STRING,
    lvl2             STRING,
    publicly_funded  INT,
    accessrights      STRING,
    gold            INT,
    green          INT,
    green_with_license          INT,
    hybrid           INT,
    bronze         INT,
    diamond             INT,
    transformative  INT,
    peer_reviewed   STRING
)
CLUSTERED BY (hist_date) INTO 100 buckets  stored as orc tblproperties ('transactional' = 'true');

drop table if exists ${hist_db_name}.historical_snapshots_tmp purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_tmp
(
    hist_date        STRING,
    total            INT,
    type             STRING,
    publicly_funded  INT,
    accessrights      STRING,
    gold            INT,
    green          INT,
    green_with_license          INT,
    hybrid           INT,
    bronze         INT,
    diamond             INT,
    transformative  INT,
    peer_reviewed   STRING
)
CLUSTERED BY (hist_date) INTO 100 buckets  stored as orc tblproperties ('transactional' = 'true');

drop table if exists ${hist_db_name}.historical_snapshots_irish_tmp purge;

CREATE TABLE ${hist_db_name}.historical_snapshots_irish_tmp
(
    hist_date        STRING,
    total            INT,
    type             STRING,
    publicly_funded  INT,
    accessrights      STRING,
    gold            INT,
    green          INT,
    green_with_license          INT,
    hybrid           INT,
    bronze         INT,
    diamond             INT,
    transformative  INT,
    peer_reviewed   STRING
)
CLUSTERED BY (hist_date) INTO 100 buckets  stored as orc tblproperties ('transactional' = 'true');