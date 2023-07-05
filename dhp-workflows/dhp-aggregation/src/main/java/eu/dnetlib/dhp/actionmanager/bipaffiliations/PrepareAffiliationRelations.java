
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.actionmanager.bipaffiliations.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.Dataset;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.bipmodel.BipScore;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

/**
 * created the Atomic Action for each tipe of results
 */
public class PrepareAffiliationRelations implements Serializable {

    private static final String DOI = "doi";
    private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <I extends Result> void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        PrepareAffiliationRelations.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/actionmanager/bipaffiliations/input_actionset_parameter.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("inputPath");
        log.info("inputPath {}: ", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath {}: ", outputPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
            conf,
            isSparkSessionManaged,
            spark -> {
                Constants.removeOutputDir(spark, outputPath);
                prepareAffiliationRelations(spark, inputPath, outputPath);
            });
    }

    private static <I extends Result> void prepareAffiliationRelations(SparkSession spark, String inputPath, String outputPath) {

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<AffiliationRelationDeserializer> bipDeserializeJavaRDD = sc
            .textFile(inputPath)
            .map(item -> OBJECT_MAPPER.readValue(item, AffiliationRelationDeserializer.class));

//        for(AffiliationRelationDeserializer rel: bipDeserializeJavaRDD.collect()){
//            System.out.println(rel);
//        }
        JavaRDD<AffiliationRelationModel> affiliationRelations =
            bipDeserializeJavaRDD.flatMap(entry ->
                entry.getMatchings().stream().flatMap(matching ->
                matching.getRorId().stream().map( rorId -> new AffiliationRelationModel(
                    entry.getDoi(),
                    rorId,
                    matching.getConfidence()
                ))).collect(Collectors.toList()).iterator());

        for(AffiliationRelationModel rel: affiliationRelations.collect()){
            System.out.println(rel);
        }
//        Dataset<AffiliationRelationModel> relations = spark
//            .createDataset(bipDeserializeJavaRDD.flatMap(entry -> {
////                        entry.keySet().stream().map(key -> {
//                AffiliationRelationModel rel = new AffiliationRelationModel(entry.getDoi())
//                    System.out.println(entry);
//                    return entry;
////                    BipScore bs = new BipScore();
////                    bs.setId(key);
////                    bs.setScoreList(entry.get(key));
////                    return bs;
//                }).collect(Collectors.toList()).iterator()).rdd(), Encoßders.bean(AffiliationRelationModel.class));

//        bipScores
//
//                .map((MapFunction<BipScore, Result>) bs -> {
//                    Result ret = new Result();
//
//                    ret.setId(bs.getId());
//
//                    ret.setMeasures(getMeasure(bs));
//
//                    return ret;
//                }, Encoders.bean(Result.class))
//                .toJavaRDD()
//                .map(p -> new AtomicAction(Result.class, p))
//                .mapToPair(
//                        aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
//                                new Text(OBJECT_MAPPER.writeValueAsString(aa))))
//                .saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

//    }
//
//    private static List<Measure> getMeasure(BipScore value) {
//        return value
//                .getScoreList()
//                .stream()
//                .map(score -> {
//                    Measure m = new Measure();
//                    m.setId(score.getId());
//                    m
//                            .setUnit(
//                                    score
//                                            .getUnit()
//                                            .stream()
//                                            .map(unit -> {
//                                                KeyValue kv = new KeyValue();
//                                                kv.setValue(unit.getValue());
//                                                kv.setKey(unit.getKey());
//                                                kv
//                                                        .setDataInfo(
//                                                                OafMapperUtils
//                                                                        .dataInfo(
//                                                                                false,
//                                                                                UPDATE_DATA_INFO_TYPE,
//                                                                                true,
//                                                                                false,
//                                                                                OafMapperUtils
//                                                                                        .qualifier(
//                                                                                                UPDATE_MEASURE_BIP_CLASS_ID,
//                                                                                                UPDATE_CLASS_NAME,
//                                                                                                ModelConstants.DNET_PROVENANCE_ACTIONS,
//                                                                                                ModelConstants.DNET_PROVENANCE_ACTIONS),
//                                                                                ""));
//                                                return kv;
//                                            })
//                                            .collect(Collectors.toList()));
//                    return m;
//                })
//                .collect(Collectors.toList());
    }
}
