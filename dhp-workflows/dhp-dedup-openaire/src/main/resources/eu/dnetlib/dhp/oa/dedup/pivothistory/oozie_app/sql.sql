
CREATE TABLE `${pivot_history_db}`.`dataset_new` STORED AS PARQUET AS
WITH pivots (
    SELECT property.value AS id, '${new_graph_date}' AS usedIn  FROM `${new_graph_db}`.`relation`
    LEFT SEMI JOIN `${new_graph_db}`.`dataset` ON relation.source = dataset.id
    LATERAL VIEW EXPLODE(properties) AS property WHERE relClass = 'isMergedIn' AND property.key = 'pivot'
UNION
    SELECT id, usedIn FROM `${pivot_history_db}`.`dataset` LATERAL VIEW EXPLODE(usages) AS usedIn
)
SELECT id, min(usedIn) as firstUsage, max(usedIn) as lastUsage, collect_set(usedIn) as usages
    FROM pivots
    GROUP BY id; /*EOS*/
CREATE TABLE `${pivot_history_db}`.`publication_new` STORED AS PARQUET AS
WITH pivots (
    SELECT property.value AS id, '${new_graph_date}' AS usedIn  FROM `${new_graph_db}`.`relation`
    LEFT SEMI JOIN `${new_graph_db}`.`publication` ON relation.source = publication.id
    LATERAL VIEW EXPLODE(properties) AS property WHERE relClass = 'isMergedIn' AND property.key = 'pivot'
UNION
    SELECT id, usedIn FROM `${pivot_history_db}`.`publication` LATERAL VIEW EXPLODE(usages) AS usedIn
)
SELECT id, min(usedIn) as firstUsage, max(usedIn) as lastUsage, collect_set(usedIn) as usages
    FROM pivots
    GROUP BY id; /*EOS*/
CREATE TABLE `${pivot_history_db}`.`software_new` STORED AS PARQUET AS
WITH pivots (
    SELECT property.value AS id, '${new_graph_date}' AS usedIn  FROM `${new_graph_db}`.`relation`
    LEFT SEMI JOIN `${new_graph_db}`.`software` ON relation.source = software.id
    LATERAL VIEW EXPLODE(properties) AS property WHERE relClass = 'isMergedIn' AND property.key = 'pivot'
UNION
    SELECT id, usedIn FROM `${pivot_history_db}`.`software` LATERAL VIEW EXPLODE(usages) AS usedIn
)
SELECT id, min(usedIn) as firstUsage, max(usedIn) as lastUsage, collect_set(usedIn) as usages
    FROM pivots
    GROUP BY id; /*EOS*/
CREATE TABLE `${pivot_history_db}`.`otherresearchproduct_new` STORED AS PARQUET AS
WITH pivots (
    SELECT property.value AS id, '${new_graph_date}' AS usedIn  FROM `${new_graph_db}`.`relation`
     LEFT SEMI JOIN `${new_graph_db}`.`otherresearchproduct` ON relation.source = otherresearchproduct.id
     LATERAL VIEW EXPLODE(properties) AS property WHERE relClass = 'isMergedIn' AND property.key = 'pivot'
UNION
    SELECT id, usedIn FROM `${pivot_history_db}`.`otherresearchproduct` LATERAL VIEW EXPLODE(usages) AS usedIn
)
SELECT id, min(usedIn) as firstUsage, max(usedIn) as lastUsage, collect_set(usedIn) as usages
    FROM pivots
    GROUP BY id; /*EOS*/


DROP TABLE IF EXISTS `${pivot_history_db}`.`dataset_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`dataset` RENAME TO `${pivot_history_db}`.`dataset_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`dataset_new` RENAME TO `${pivot_history_db}`.`dataset`; /*EOS*/

DROP TABLE IF EXISTS `${pivot_history_db}`.`publication_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`publication` RENAME TO `${pivot_history_db}`.`publication_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`publication_new` RENAME TO `${pivot_history_db}`.`publication`; /*EOS*/

DROP TABLE IF EXISTS `${pivot_history_db}`.`software_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`software` RENAME TO `${pivot_history_db}`.`software_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`software_new` RENAME TO `${pivot_history_db}`.`software`; /*EOS*/

DROP TABLE IF EXISTS `${pivot_history_db}`.`otherresearchproduct_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`otherresearchproduct` RENAME TO `${pivot_history_db}`.`otherresearchproduct_old`; /*EOS*/
ALTER TABLE `${pivot_history_db}`.`otherresearchproduct_new` RENAME TO `${pivot_history_db}`.`otherresearchproduct`; /*EOS*/
