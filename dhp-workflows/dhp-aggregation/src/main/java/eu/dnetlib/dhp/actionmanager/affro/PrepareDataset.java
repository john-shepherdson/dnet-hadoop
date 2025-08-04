package eu.dnetlib.dhp.actionmanager.affro;


import static eu.dnetlib.dhp.actionmanager.affro.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;

import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.actionmanager.affro.beans.Affiliation;
import eu.dnetlib.dhp.actionmanager.affro.beans.Author;
import eu.dnetlib.dhp.actionmanager.affro.beans.IISModel;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
//import org.apache.spark.sql.*;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.DHPUtils;

//It prepares the dataset made from the various inputs to be used by affro
//inputs: oalex from /data/openalex-snapshot
//input oaire from a previous execution of the pipeline
//input publishers files
//input iis table that stores pdf extraction data
public class PrepareDataset implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(PrepareDataset.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        PrepareDataset.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/actionmanager/affro/input_preparedataset_parameter.json"));
        log.info("read parameter file");

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String oalexPath = parser.get("oalexPath");
        log.info("oalexPath: {}", oalexPath);

        final String oairePath = parser.get("oairePath");
        log.info("oairePath: {}", oairePath);

        final String publishersPath = parser.get("publishersPath");
        log.info("publishersPath: {}", publishersPath);

        final String iisPath = parser.get("iisPath");
        log.info("iisPath: {}", iisPath);

        final String oldMatches = parser.get("oldMatches");
        log.info("oldMatches: {}", oldMatches);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);



        final Boolean startFromScratch = Optional
                .ofNullable(parser.get("applyOnAll"))
                .map(Boolean::valueOf)
                .orElse(Boolean.FALSE);

        String hiveMetastoreUris = parser.get("hiveMetastoreUris");
        log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", hiveMetastoreUris);

        runWithSparkHiveSession(
//        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Constants.removeOutputDir(spark, outputPath);
                    prepareDataset(
                            spark, oalexPath, oairePath, iisPath, publishersPath, outputPath, oldMatches,
                            startFromScratch);
                });
    }

    private static void prepareDataset(SparkSession spark, String oalexPath, String oairePath, String iisPath,
                                       String publishersPath, String workingDir, String oldMatches,
                                       Boolean startFromScratch) {
        // start with oalex. read from the snapshot in the schema needed for this task
        // Function to compute MD5 hash with prefix
        spark
                .udf()
                .register(
                        "md5HashWithPrefix", (String doi) -> "50|doi_________::" + DHPUtils.md5(doi), DataTypes.StringType);
        spark
                .udf()
                .register(
                        "addResultPrefix", (String id) -> "50|" + id, DataTypes.StringType);

        spark
                .udf()
                .register(
                        "selectId", (String doi, String id) -> StringUtils.isNotEmpty(id) ? id : "50|doi_________::" + DHPUtils.md5(StringUtils.substringAfter(doi,"doi.org/")), DataTypes.StringType);

        //the output model for all the datasets will be:
        //id : the openaire identifier for the resource
        //authors.fullname the fullname of the author
        //authors.raw_affiliation_strings the list of raw_affiliation_strings associated to the author
        //todo decide if we want also to include authors.pid not from oalex but from the other sources
        Dataset<Row> oalex = spark
                .read()
                .schema(OALEX_SCHEMA)
                .json(oalexPath)
                .filter(col("doi").isNotNull())
                .withColumn("id",  expr("md5HashWithPrefix(doi)"))
                .select(col("id"),
                 explode( col("authorships")).alias("author"))
                .withColumn("fullname", col("author.author.display_name"))
                .withColumn("raw_affiliation_strings", col("author.raw_affiliation_strings"))
                .select(col("id"), col("fullname"),
                         explode(col("raw_affiliation_strings")).alias("raw_affiliation_string"))
                .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .withColumn("corresponding", lit(null))
                .withColumn("contributor_roles", lit(null))
                .select(col("id"), col("fullname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"))
                .as(RowEncoder.apply(DATASET_SCHEMA));
        oalex.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "/exploded/oalex");

        Dataset<Row> oaire_entities =
                spark.createDataFrame(Collections.emptyList(), GRAPH_SCHEMA);
        for(EntityType entity: ModelSupport.entityTypes.keySet()) {
            if (ModelSupport.isResult(entity)) {
                oaire_entities = oaire_entities.union(spark.read().schema(GRAPH_SCHEMA).json(oairePath + "/" + entity.name()));

            }
        }
            Dataset<Row> oaire = oaire_entities
                    .select(col("id"), explode(col("author")).alias("author"))
                    .select(col("id"), col("author"), explode(col("author.rawAffiliationString")).alias("raw_affiliation_string"))
                    .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                    .withColumn("fullname", col("author.fullName"))
                    .drop("author")
                    .withColumn("corresponding", lit(null))
                    .withColumn("contributor_roles", lit(null))
                    .select(col("id"), col("fullname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"));
        oaire.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "/exploded/oaire");

        Dataset<Row> iis =
                spark.sql(IIS_QUERY)
//        spark.read().schema(Encoders.bean(IISModel.class).schema())
//                .json(iisPath)
                .as(Encoders.bean(IISModel.class))
                  .filter((FilterFunction<IISModel>) value -> Optional.ofNullable(value.getAuthors()).isPresent() &&
                  !value.getAuthors().isEmpty() &&
                          Optional.ofNullable(value.getAffiliations()).isPresent() &&
                          !value.getAffiliations().isEmpty())

                .flatMap((FlatMapFunction<IISModel, Row>) value -> {
                    List<Row> ret = new ArrayList<>();
                    value.getAuthors().stream().forEach(author -> ret.addAll(
                            getAuthorLines(value.getId(), author, value.getAffiliations())));
                    return ret.iterator();
                    }
                , RowEncoder.apply(DATASET_SCHEMA))
                        .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .select(col("id"), col("fullname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"));
        iis.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "/exploded/iis");

        Dataset<Row> publishers = spark.read().schema(PUBLISHER_SCHEMA).json(publishersPath)
                .filter( col("success").equalTo(true))
                .withColumn("authors", col("parsing_output.authors"))
                .select( col("id"), col("doi"),
                        explode(col("authors")).alias("author"))
                .withColumn("graphId" , col("id"))
                .drop(col("id"))
                .withColumn("fullname", col("author.name.full"))
                .withColumn("raw_affiliation_strings",  col("author.raw_affiliations"))
                .withColumn("corresponding", col("author.corresponding"))
                .withColumn("contributor_roles", col("author.contributor_roles"))
                .drop(col("author"))
                .select(col("graphId"),col("doi"),col("fullname"), explode(col("raw_affiliation_strings")).alias("raw_affiliation_string")
                ,col("corresponding"), col("contributor_roles"))
                .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .withColumn("id",  expr("selectId(doi, graphId)"))
                .drop(col("graphId"))
                .drop(col("doi"))
                .select("id","fullname","raw_affiliation_string","corresponding","contributor_roles");
        publishers.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "/exploded/publishers");

        Dataset<Row> inputDataset = oalex.union(oaire).union(publishers).union(iis)
                .distinct()
                ;
//
//
//        inputDataset.select( col("raw_affiliation_string"))
//                .distinct()
//                .write()
//                .mode(SaveMode.Overwrite)
//                .option("compression", "gzip")
//                .json(workingDir + "/all_strings");
//
//        Dataset<Row> alreadyMatched = spark.createDataFrame(Collections.emptyList(), AFFILIATION_SCHEMA);
//
//
//        if (!startFromScratch){
//            alreadyMatched = spark.read().schema(AFFILIATION_SCHEMA)
//                    .json(oldMatches);
//        }
//        Dataset<Row> affStrings = spark.read().schema(AFFILIATION_STRING_SCHEMA).json(workingDir + "/all_strings");
//        Dataset<Row> newToMatch =  affStrings.join(alreadyMatched, affStrings.col("raw_affiliation_string").equalTo(alreadyMatched.col("Affiliation")), "left")
//                .filter( col("Affiliation").isNull())
//                .select("raw_affiliation_string")
//                .distinct();
//
//        newToMatch.write()
//                .mode(SaveMode.Overwrite)
//                .option("compression", "gzip")
//                .json(workingDir+"/toMatch" );

    }

    private static List<Row> getAuthorLines(String id, Author author, List<Affiliation> affiliations) {
        List<Integer> affiliationPositions = author.getAffiliationpositions();
        if (Optional.ofNullable(affiliationPositions).isPresent() && !affiliationPositions.isEmpty())
            return affiliationPositions.stream().map(pos -> {
                if(pos < affiliations.size())
                    return RowFactory.create(id, author.getAuthorfullname(), affiliations.get(pos).getRawtext(), null, null);
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        else
            return new ArrayList<>();
    }




}

