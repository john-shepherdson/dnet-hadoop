
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.actionmanager.bipaffiliations.model.*;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
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
import scala.Tuple2;

/**
 * created the Atomic Action for each tipe of results
 */
public class PrepareAffiliationRelations implements Serializable {

    private static final String DOI = "doi";
    private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ID_PREFIX = "50|doi_________::";

    private static final String TRUST = "0.91";

    public static final String BIP_AFFILIATIONS_CLASSID = "sysimport:crosswalk:bipaffiliations";
    public static final String BIP_AFFILIATIONS_CLASSNAME = "Imported from BIP! Affiliations";

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

        JavaRDD<AffiliationRelationDeserializer> affiliationRelationsDeserializeRDD = sc
            .textFile(inputPath)
            .map(item -> OBJECT_MAPPER.readValue(item, AffiliationRelationDeserializer.class));

//        for(AffiliationRelationDeserializer rel: bipDeserializeJavaRDD.collect()){
//            System.out.println(rel);
//        }

        Dataset<AffiliationRelationModel> affiliationRelations =
            spark.createDataset(
                affiliationRelationsDeserializeRDD.flatMap(entry ->
                    entry.getMatchings().stream().flatMap(matching ->
                    matching.getRorId().stream().map( rorId -> new AffiliationRelationModel(
                        entry.getDoi(),
                        rorId,
                        matching.getConfidence()
                    ))).collect(Collectors.toList())
                    .iterator())
                .rdd(),
                Encoders.bean(AffiliationRelationModel.class));

        affiliationRelations
            .map((MapFunction<AffiliationRelationModel, Relation>) affRel -> {

                String paperId = ID_PREFIX
                        + IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", affRel.getDoi()));
                final String affId = ID_PREFIX
                        + IdentifierFactory.md5(CleaningFunctions.normalizePidValue("ror", affRel.getRorId()));

                return getRelation(paperId, affId, ModelConstants.HAS_AUTHOR_INSTITUTION);

            }, Encoders.bean(Relation.class))
            .toJavaRDD()
            .map(p -> new AtomicAction(Relation.class, p))
            .mapToPair(
                aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
                    new Text(OBJECT_MAPPER.writeValueAsString(aa))))
            .saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

    }

    public static Relation getRelation(String source, String target, String relclass) {
        Relation r = new Relation();

        r.setCollectedfrom(getCollectedFrom());
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass(relclass);
        r.setRelType(ModelConstants.RESULT_ORGANIZATION);
        r.setSubRelType(ModelConstants.AFFILIATION);
        r.setDataInfo(getDataInfo());
        return r;
    }

    public static List<KeyValue> getCollectedFrom() {
        KeyValue kv = new KeyValue();
        kv.setKey(ModelConstants.DNET_PROVENANCE_ACTIONS);
        kv.setValue(ModelConstants.DNET_PROVENANCE_ACTIONS);

        return Collections.singletonList(kv);
    }

    public static DataInfo getDataInfo() {
        DataInfo di = new DataInfo();
        di.setInferred(false);
        di.setDeletedbyinference(false);
        di.setTrust(TRUST);
        di.setProvenanceaction(
            getQualifier(
                BIP_AFFILIATIONS_CLASSID,
                BIP_AFFILIATIONS_CLASSNAME,
                ModelConstants.DNET_PROVENANCE_ACTIONS
            ));
        return di;
    }

    public static Qualifier getQualifier(String class_id, String class_name,
                                         String qualifierSchema) {
        Qualifier pa = new Qualifier();
        pa.setClassid(class_id);
        pa.setClassname(class_name);
        pa.setSchemeid(qualifierSchema);
        pa.setSchemename(qualifierSchema);
        return pa;
    }

}
