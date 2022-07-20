package eu.dnetlib.dhp.oa.graph.clean;

/**
 * @author miriam.baglioni
 * @Date 20/07/22
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;


public class CleanCountrySparkJob implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(CleanCountrySparkJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        CleanContextSparkJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/input_clean_country_parameters.json"));
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

        String country = parser.get("country");
        log.info("country: {}", country);

        String verifyParam = parser.get("verifyParam");
        log.info("verifyParam: {}", verifyParam);

        String collectedfrom = parser.get("collectedfrom");
        log.info("collectedfrom: {}", collectedfrom);

        String graphTableClassName = parser.get("graphTableClassName");
        log.info("graphTableClassName: {}", graphTableClassName);

        Class<? extends Result> entityClazz = (Class<? extends Result>) Class.forName(graphTableClassName);

        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {

                    cleanCountry(spark, country, verifyParam, inputPath, entityClazz, workingPath,collectedfrom);
                });
    }

    private static <T extends Result> void cleanCountry(SparkSession spark, String country, String verifyParam,
                                                        String inputPath, Class<T> entityClazz, String workingPath, String collectedfrom) {
        Dataset<T> res = spark
                .read()
                .textFile(inputPath)
                .map(
                        (MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
                        Encoders.bean(entityClazz))
                ;

        res.map((MapFunction<T, T>) r -> {
            for(StructuredProperty p: r.getPid()){
                if (p.getQualifier().getClassid().equalsIgnoreCase("doi") &&
                p.getValue().startsWith(verifyParam) && r.getCollectedfrom().stream().anyMatch(cf -> cf.getValue().equalsIgnoreCase(collectedfrom))) {

                    r
                            .setCountry(
                                    r
                                            .getCountry()
                                            .stream()
                                            .filter(
                                                    c -> !c.getClassid()
                                                            .equalsIgnoreCase(country))
                                            .collect(Collectors.toList()));
                }
            }

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

