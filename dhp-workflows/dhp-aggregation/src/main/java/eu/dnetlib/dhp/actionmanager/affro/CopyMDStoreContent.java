package eu.dnetlib.dhp.actionmanager.affro;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.col;
import java.io.Serializable;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.MAPPER;

public class CopyMDStoreContent implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(PrepareDataset.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        PrepareDataset.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/actionmanager/affro/input_copydataset_parameter.json"));
        log.info("read parameter file");

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String datasourceMDVersion = parser.get("datasourceMDVersion");
        log.info("datasourceMDVersion: {}", datasourceMDVersion);
        final MDStoreVersion datasourceMdStoreVersion = MAPPER.readValue(datasourceMDVersion, MDStoreVersion.class);
        final String datasourceBasePath = datasourceMdStoreVersion.getHdfsPath() + "/store";
        log.info("datasourceBasePath: {}", datasourceBasePath);


        SparkConf conf = new SparkConf();

        runWithSparkSession(
//        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    copyContent(spark, datasourceBasePath, outputPath);
                });
}

    private static void copyContent(SparkSession spark, String datasourceBasePath, String outputPath) {
        spark.read().schema(eu.dnetlib.dhp.actionmanager.affro.Constants.GRAPH_SCHEMA)
                .json(datasourceBasePath)
                .where(col("id").isNotNull())
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);

        }
    }
