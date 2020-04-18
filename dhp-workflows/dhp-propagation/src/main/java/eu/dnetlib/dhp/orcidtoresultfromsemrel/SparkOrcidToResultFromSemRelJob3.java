package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class SparkOrcidToResultFromSemRelJob3 {
    private static final Logger log = LoggerFactory.getLogger(SparkOrcidToResultFromSemRelJob3.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils.toString(SparkOrcidToResultFromSemRelJob3.class
                .getResourceAsStream("/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_orcidtoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);


        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String possibleUpdates = parser.get("possibleUpdatesPath");
        log.info("possibleUpdatesPath: {}", possibleUpdates);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final Boolean saveGraph = Optional
                .ofNullable(parser.get("saveGraph"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("saveGraph: {}", saveGraph);

        Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
                    if(isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    if(saveGraph)
                        execPropagation(spark, possibleUpdates, inputPath, outputPath, resultClazz);
                });

    }

    private static <R extends Result> void execPropagation(SparkSession spark, String possibleUpdatesPath, String inputPath,
                                                           String outputPath, Class<R> resultClazz ) {

        //read possible updates (resultId and list of possible orcid to add
        Dataset<ResultOrcidList> possible_updates = readAssocResultOrcidList(spark, possibleUpdatesPath);
        //read the result we have been considering
        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
        //make join result left_outer with possible updates

        result.joinWith(possible_updates, result.col("id").equalTo(possible_updates.col("resultId")),
                    "left_outer")
                    .map(value -> {
                        R ret = value._1();
                        Optional<ResultOrcidList> rol = Optional.ofNullable(value._2());
                        if(rol.isPresent()) {
                            List<Author> toenrich_author = ret.getAuthor();
                            List<AutoritativeAuthor> autoritativeAuthors = rol.get().getAuthorList();
                            for(Author author: toenrich_author){
                                if (!containsAllowedPid(author)){
                                    enrichAuthor(author, autoritativeAuthors);
                                }
                            }
                        }

                        return ret;
                    }, Encoders.bean(resultClazz))
                    .toJSON()
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("compression","gzip")
                    .text(outputPath);


    }

    private static Dataset<ResultOrcidList> readAssocResultOrcidList(SparkSession spark, String relationPath) {
        return spark
                .read()
                .textFile(relationPath)
                .map(value -> OBJECT_MAPPER.readValue(value, ResultOrcidList.class), Encoders.bean(ResultOrcidList.class));
    }

    private static void enrichAuthor(Author a, List<AutoritativeAuthor> au){
        for (AutoritativeAuthor aa: au){
            if(enrichAuthor(aa, a)){
                return;
            }
        }

    }



    private static boolean enrichAuthor(AutoritativeAuthor autoritative_author, Author author) {
        boolean toaddpid = false;

        if (StringUtils.isNoneEmpty(autoritative_author.getSurname())) {
            if (StringUtils.isNoneEmpty(author.getSurname())) {
                if (autoritative_author.getSurname().trim().equalsIgnoreCase(author.getSurname().trim())) {

                    //have the same surname. Check the name
                    if (StringUtils.isNoneEmpty(autoritative_author.getName())) {
                        if (StringUtils.isNoneEmpty(author.getName())) {
                            if (autoritative_author.getName().trim().equalsIgnoreCase(author.getName().trim())) {
                                toaddpid = true;
                            }
                            //they could be differently written (i.e. only the initials of the name in one of the two
                            if (autoritative_author.getName().trim().substring(0, 0).equalsIgnoreCase(author.getName().trim().substring(0, 0))) {
                                toaddpid = true;
                            }
                        }
                    }
                }
            }
        }
        if (toaddpid){
            StructuredProperty p = new StructuredProperty();
            p.setValue(autoritative_author.getOrcid());
            p.setQualifier(getQualifier(PROPAGATION_AUTHOR_PID, PROPAGATION_AUTHOR_PID ));
            p.setDataInfo(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME));
            author.addPid(p);

        }
        return toaddpid;

    }




    private static boolean containsAllowedPid(Author a) {
        for (StructuredProperty pid : a.getPid()) {
            if (PROPAGATION_AUTHOR_PID.equals(pid.getQualifier().getClassid())) {
                return true;
            }
        }
        return false;
    }

}
