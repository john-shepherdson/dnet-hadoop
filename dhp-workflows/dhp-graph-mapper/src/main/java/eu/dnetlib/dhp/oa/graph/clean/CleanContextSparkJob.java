package eu.dnetlib.dhp.oa.graph.clean;

/**
 * @author miriam.baglioni
 * @Date 28/06/22
 */


import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Result;

public class CleanContextSparkJob implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(CleanContextSparkJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        CleanContextSparkJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/input_clean_context_parameters.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("inputPath");
        log.info("inputPath: {}", inputPath);

        String workingPath = parser.get("workingPath");
        log.info("workingPath: {}", workingPath);

        String contextId = parser.get("contextId");
        log.info("contextId: {}", contextId);

        String verifyParam = parser.get("verifyParam");
        log.info("verifyParam: {}", verifyParam);

        String graphTableClassName = parser.get("graphTableClassName");
        log.info("graphTableClassName: {}", graphTableClassName);

        Class<? extends Result> entityClazz = (Class<? extends Result>) Class.forName(graphTableClassName);

        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {

                    cleanContext(spark, contextId, verifyParam, inputPath, entityClazz, workingPath);
                });
    }

    private static <T extends Result> void cleanContext(SparkSession spark, String contextId, String verifyParam,
                                                        String inputPath, Class<T> entityClazz, String workingPath) {
        Dataset<T> res = spark
                .read()
                .textFile(inputPath)
                .map(
                        (MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
                        Encoders.bean(entityClazz));

        res.map((MapFunction<T, T>) r -> {
                    if (!r
                            .getTitle()
                            .stream()
                            .filter(
                                    t -> t
                                            .getQualifier()
                                            .getClassid()
                                            .equalsIgnoreCase(ModelConstants.MAIN_TITLE_QUALIFIER.getClassid()))
                            .anyMatch(t -> t.getValue().toLowerCase().startsWith(verifyParam.toLowerCase()))) {
                        return r;
                    }
                    r
                            .setContext(
                                    r
                                            .getContext()
                                            .stream()
                                            .filter(
                                                    c -> !c.getId().split("::")[0]
                                                            .equalsIgnoreCase(contextId))
                                            .collect(Collectors.toList()));
                    return r;
                }, Encoders.bean(entityClazz))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .json(workingPath);

        spark
                .read()
                .textFile(workingPath)
                .map(
                        (MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
                        Encoders.bean(entityClazz))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .json(inputPath);
    }
}
