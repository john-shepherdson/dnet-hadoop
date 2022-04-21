package eu.dnetlib.dhp.bulktag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.CommunityConfigurationFactory;
import eu.dnetlib.dhp.bulktag.community.ProtoMap;
import eu.dnetlib.dhp.bulktag.community.QueryInformationSystem;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkEoscTag {
    private static final Logger log = LoggerFactory.getLogger(SparkEoscTag.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkEoscTag.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/bulktag/input_eosctag_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    execEoscTag(spark, inputPath);

                });
    }

    private static void execEoscTag(SparkSession spark, String inputPath) {
        //search for notebook
        //subject contiene jupyter.
        //esistono python e notebook nei subject non necessariamente nello stesso
        //si cerca fra i prodotto di tipo software
        Dataset<Software> sw = readPath(spark, inputPath + "/software", Software.class)

    }
}
