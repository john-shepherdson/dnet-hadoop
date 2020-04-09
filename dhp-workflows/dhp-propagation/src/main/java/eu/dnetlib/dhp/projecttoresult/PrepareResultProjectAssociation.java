package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.PrepareDatasourceCountryAssociation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskResultLost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.TRUE;
import static eu.dnetlib.dhp.PropagationConstant.createOutputDirs;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareResultProjectAssociation {
    private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception{

        String jsonConfiguration = IOUtils.toString(PrepareDatasourceCountryAssociation.class
                .getResourceAsStream("/eu/dnetlib/dhp/countrypropagation/input_projecttoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath {}: ", outputPath);

        boolean writeUpdates = TRUE.equals(parser.get("writeUpdate"));
        log.info("writeUpdates: {} ", writeUpdates);

        boolean saveGraph = TRUE.equals(parser.get("saveGraph"));
        log.info("saveGraph {}", saveGraph);

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
                    createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

                });



    }
}
