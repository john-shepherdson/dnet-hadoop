package eu.dnetlib.dhp.bypassactionset.opencitations;


import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import eu.dnetlib.dhp.schema.common.ModelConstants;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;



public class SparkUpdateOCRels implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkUpdateOCRels.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(final String[] args) throws IOException, ParseException {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                Objects
                                        .requireNonNull(
                                                SparkUpdateOCRels.class
                                                        .getResourceAsStream(
                                                                "/eu/dnetlib/dhp/actionmanager/opencitations/as_parameters.json"))));

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);

        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("inputPath");
        log.info("inputPath {}", inputPath.toString());

        final String outputPath = parser.get("outputPath");
        log.info("outputPath {}", outputPath);

        final boolean shouldDuplicateRels =
                Optional.ofNullable(parser.get("shouldDuplicateRels"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.FALSE);

        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark ->
                    addOCRelations(spark, inputPath, outputPath, shouldDuplicateRels)
                );

    }

    private static void addOCRelations(SparkSession spark, String inputPath, String outputPath,
                                       boolean shouldDuplicateRels) {
        spark
                .sqlContext()
                .createDataset(spark.sparkContext().textFile(inputPath + "/*", 6000), Encoders.STRING())
                .flatMap(
                        (FlatMapFunction<String, Relation>) value -> createRelation(value, shouldDuplicateRels).iterator(),
                        Encoders.bean(Relation.class))
                .filter((FilterFunction<Relation>) value -> value != null)
                .write()
                .mode(SaveMode.Append)
                .option("compression", "gzip")
                .json(outputPath);

    }

    private static List<Relation> createRelation(String value, boolean duplicate) {
        String[] line = value.split(",");
        if (!line[1].startsWith("10.")) {
            return new ArrayList<>();
        }
        List<Relation> relationList = new ArrayList<>();

        String citing = ID_PREFIX + IdentifierFactory.md5(CleaningFunctions.normalizePidValue(DOI, line[1]));
        final String cited = ID_PREFIX + IdentifierFactory.md5(CleaningFunctions.normalizePidValue(DOI, line[2]));

        relationList
                .addAll(
                        getRelations(
                                citing,
                                cited));

        if (duplicate && line[1].endsWith(REF_DOI)) {
            citing = ID_PREFIX + IdentifierFactory
                    .md5(CleaningFunctions.normalizePidValue(DOI, line[1].substring(0, line[1].indexOf(REF_DOI))));
            relationList.addAll(getRelations(citing, cited));
        }

        return relationList;
    }

    private static Collection<Relation> getRelations(String citing, String cited) {

        return Arrays
                .asList(
                        getRelation(citing, cited, ModelConstants.CITES),
                        getRelation(cited, citing, ModelConstants.IS_CITED_BY));
    }

    public static Relation getRelation(
            String source,
            String target,
            String relclass) {
        Relation r = new Relation();
        r.setCollectedfrom(getCollectedFrom());
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass(relclass);
        r.setRelType(ModelConstants.RESULT_RESULT);
        r.setSubRelType(ModelConstants.CITATION);
        r
                .setDataInfo(
                        getDataInfo(UPDATE_DATA_INFO_TYPE, OPENCITATIONS_CLASSID, OPENCITATIONS_CLASSNAME,
                                ModelConstants.DNET_PROVENANCE_ACTIONS, OC_TRUST, false));
        return r;
    }

    public static List<KeyValue> getCollectedFrom() {
        KeyValue kv = new KeyValue();
        kv.setKey(ModelConstants.OPENOCITATIONS_ID);
        kv.setValue(ModelConstants.OPENOCITATIONS_NAME);

        return Arrays.asList(kv);
    }



}
