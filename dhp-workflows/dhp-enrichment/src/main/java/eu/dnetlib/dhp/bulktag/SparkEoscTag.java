package eu.dnetlib.dhp.bulktag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.CommunityConfigurationFactory;
import eu.dnetlib.dhp.bulktag.community.ProtoMap;
import eu.dnetlib.dhp.bulktag.community.QueryInformationSystem;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
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

import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkEoscTag {
    private static final Logger log = LoggerFactory.getLogger(SparkEoscTag.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public final static StructuredProperty EOSC_NOTEBOOK = OafMapperUtils.structuredProperty(
            "EOSC::Jupyter Notebook", OafMapperUtils.qualifier("eosc","European Open Science Cloud",
                    ModelConstants.DNET_SUBJECT_TYPOLOGIES,ModelConstants.DNET_SUBJECT_TYPOLOGIES)
    ,OafMapperUtils.dataInfo(false, "propagation", true, false,
                    OafMapperUtils.qualifier("propagation:subject","Inferred by OpenAIRE",
                            ModelConstants.DNET_PROVENANCE_ACTIONS,ModelConstants.DNET_PROVENANCE_ACTIONS), "0.9"));

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

        final String workingPath = parser.get("workingPath");
        log.info("workingPath: {}", workingPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    execEoscTag(spark, inputPath, workingPath);

                });
    }

    private static void execEoscTag(SparkSession spark, String inputPath, String workingPath) {
        //search for notebook
        //subject contiene jupyter.
        //esistono python e notebook nei subject non necessariamente nello stesso
        //si cerca fra i prodotti di tipo software


        readPath(spark, inputPath + "/software", Software.class)
                .map((MapFunction<Software, Software>) s -> {
                    if(containsSubjectNotebook(s)){
                        s.getSubject().add(EOSC_NOTEBOOK);
                    }
                    return s;
                }, Encoders.bean(Software.class) )
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(workingPath + "/software");

        readPath(spark, workingPath + "/software" , Software.class)
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(inputPath + "/software");

    }

    private static boolean containsSubjectNotebook(Software s) {
        if(s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("jupyter")))
            return true;
        if(s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("python") &&
                sbj.getValue().toLowerCase().contains("notebook")))
            return true;
        if(s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("python")) &&
                s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("notebook")))
            return true;
        return false;
    }

    private static boolean containsTitleNotebook(Software s) {
        if (s.getTitle().stream().anyMatch(t -> t.getValue().toLowerCase().contains("jupyter") &&
                t.getValue().toLowerCase().contains("notebook")))
            return true;
        return false;
    }
}
