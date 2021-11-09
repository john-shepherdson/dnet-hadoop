package eu.dnetlib.dhp.bypassactionset;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import eu.dnetlib.dhp.bypassactionset.model.FOSDataModel;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkUpdateFOS implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkUpdateFOS.class);
    private final static String NULL = "NULL";
    private final static String DNET_RESULT_SUBJECT = "dnet:result_subject";

    public static <I extends Result> void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        SparkUpdateFOS.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/actionmanager/bipfinder/input_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);

        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("inputPath");
        log.info("inputPath {}: ", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath {}: ", outputPath);

        final String fosPath = parser.get("fosPath");
        log.info("fosPath: {}", fosPath);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        Class<I> inputClazz = (Class<I>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark ->
                        updateFos(spark, inputPath, outputPath, fosPath, inputClazz)

        );
    }

    private static <I extends Result> void updateFos(SparkSession spark, String inputPath, String outputPath,
                                                           String bipScorePath, Class<I> inputClazz) {

        Dataset<I> results = readPath(spark, inputPath, inputClazz);
        Dataset<FOSDataModel> bipScores = readPath(spark, bipScorePath, FOSDataModel.class);

        results.joinWith(bipScores, results.col("id").equalTo(bipScores.col("id")), "left")
                .map((MapFunction<Tuple2<I,FOSDataModel>, I>) value -> {
                    if (!Optional.ofNullable(value._2()).isPresent()){
                        return value._1();
                    }
                    value._1().getSubject().addAll(getSubjects(value._2()));
                    return value._1();
                }, Encoders.bean(inputClazz))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);

    }

    private static List<StructuredProperty> getSubjects(FOSDataModel fos) {
        return Arrays.asList(getSubject(fos.getLevel1()), getSubject(fos.getLevel2()), getSubject(fos.getLevel3()))
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static StructuredProperty getSubject(String sbj) {
        if (sbj.equals(NULL))
            return null;
        StructuredProperty sp = new StructuredProperty();
        sp.setValue(sbj);
        sp.setQualifier(getQualifier(FOS_CLASS_ID, FOS_CLASS_NAME, DNET_RESULT_SUBJECT));
        sp.setDataInfo(getDataInfo(UPDATE_DATA_INFO_TYPE,
                UPDATE_SUBJECT_FOS_CLASS_ID,
                UPDATE_SUBJECT_FOS_CLASS_NAME,
                ModelConstants.DNET_PROVENANCE_ACTIONS, ""));
        return sp;

    }


}
