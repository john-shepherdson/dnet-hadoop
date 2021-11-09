package eu.dnetlib.dhp.bypassactionset.fos;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bypassactionset.model.FOSDataModel;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.bypassactionset.Utils.getIdentifier;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class PrepareFOSSparkJob implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(PrepareFOSSparkJob.class);


    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        PrepareFOSSparkJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/bypassactionset/distribute_fos_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String sourcePath = parser.get("sourcePath");
        log.info("sourcePath: {}", sourcePath);



        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);



        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    distributeFOSdois(
                            spark,
                            sourcePath,

                            outputPath
                            );
                });
    }

    private static void distributeFOSdois(SparkSession spark, String sourcePath, String outputPath) {
        Dataset<FOSDataModel> fosDataset = readPath(spark, sourcePath, FOSDataModel.class);

        fosDataset.flatMap((FlatMapFunction<FOSDataModel, FOSDataModel>) v -> {
            List<FOSDataModel> fosList = new ArrayList<>();
            final String level1 = v.getLevel1();
            final String level2 = v.getLevel2();
            final String level3 = v.getLevel3();
            Arrays.stream(v.getDoi().split("\u0002")).forEach(d ->
                    fosList.add(FOSDataModel.newInstance(getIdentifier(d), level1, level2, level3)));
            return fosList.iterator();
        }, Encoders.bean(FOSDataModel.class))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);
    }




}
