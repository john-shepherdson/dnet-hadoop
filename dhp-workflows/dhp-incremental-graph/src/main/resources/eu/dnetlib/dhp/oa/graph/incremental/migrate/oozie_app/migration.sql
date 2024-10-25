INSERT OVERWRITE DIRECTORY '${outputPath}/datasource'
    USING json OPTIONS ('compression' 'gzip')
    SELECT * FROM `${hiveDbName}`.`datasource`; /* EOS */

INSERT OVERWRITE DIRECTORY '${outputPath}/organization'
    USING json OPTIONS ('compression' 'gzip')
    SELECT * FROM `${hiveDbName}`.`organization`; /* EOS */

INSERT OVERWRITE DIRECTORY '${outputPath}/project'
    USING json OPTIONS ('compression' 'gzip')
    SELECT * FROM `${hiveDbName}`.`project`; /* EOS */