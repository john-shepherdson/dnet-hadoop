package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.SparkCountryPropagationJob;
import eu.dnetlib.dhp.resulttoorganizationfrominstrepo.SparkResultToOrganizationFromIstRepoJob;
import eu.dnetlib.dhp.utils.ORCIDAuthorEnricherResult;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkCopyEnrichedPublisherAuthors implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob.class);
    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        SparkCopyEnrichedPublisherAuthors.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/wf/subworkflows/enrich/publisher/input_propagation_parameter.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String workingDir = parser.get("workingDir");
        log.info("workingDir: {}", workingDir);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> copyEnrichedAuthors(spark, workingDir, outputPath));
    }


    private static void copyEnrichedAuthors(SparkSession spark, String workingDir, String persistedPath) {
        spark.read().schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
                .parquet(workingDir + "/publication_matched")
                .selectExpr("id as doi", "enriched_author")
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(persistedPath + "/publisherEnrichedAuthors");
    }
}
