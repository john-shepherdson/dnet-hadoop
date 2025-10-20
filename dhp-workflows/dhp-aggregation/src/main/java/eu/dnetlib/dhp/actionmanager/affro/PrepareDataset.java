package eu.dnetlib.dhp.actionmanager.affro;


import static eu.dnetlib.dhp.actionmanager.affro.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.common.person.Constants.removePrefixUrl;
import static eu.dnetlib.dhp.utils.DHPUtils.MAPPER;
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


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
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

        final String dataciteInputPath = parser.get("dataciteInputPath");
        log.info("dataciteInputPath: {}", dataciteInputPath);


        final String crossrefInputPath = parser.get("crossrefInputPath");
        log.info("crossrefInputPath: {}", crossrefInputPath);


        final String pubmedInputPath = parser.get("pubmedInputPath");
        log.info("pubmedInputPath: {}", pubmedInputPath);

        final String iisPath = parser.get("iisPath");
        log.info("iisPath: {}", iisPath);

        final String oldMatches = parser.get("oldMatches");
        log.info("oldMatches: {}", oldMatches);

        final String workingDir = parser.get("outputPath");
        log.info("workingDir: {}", workingDir);

        final Boolean importIIS = Optional.ofNullable(parser.get("importiis"))
                .map(Boolean::valueOf)
                .orElse(Boolean.FALSE);

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

                    prepareDataset(
                            spark, oalexPath, oairePath, iisPath , publishersPath,
                            dataciteInputPath, crossrefInputPath, pubmedInputPath, workingDir, oldMatches,
                            startFromScratch, importIIS);
                });
    }

    private static void prepareDataset(SparkSession spark, String oalexPath, String oairePath, String iisPath,
                                       String publishersPath, String datacitePath, String crossrefPath,
                                       String pubmedPath,
                                       String workingDir, String oldMatches,
                                       Boolean startFromScratch, Boolean importIIS) {
        // start with oalex. read from the snapshot in the schema needed for this task
        // Function to compute MD5 hash with prefix
        spark
                .udf()
                .register(
                        "md5HashWithPrefix", (String doi) ->  "50|doi_________::" + DHPUtils.md5(removePrefixUrl(doi)), DataTypes.StringType);
        spark
                .udf()
                .register(
                        "selectId", (String doi, String id) -> StringUtils.isNotEmpty(id) ? "50|" + id : "50|doi_________::" + DHPUtils.md5(removePrefixUrl(doi)), DataTypes.StringType);

        spark
                .udf()
                .register(
                        "concat", (String firstName, String familyName) -> familyName + ", " + firstName , DataTypes.StringType);


        Dataset<Row> datacite = getSelectGraphSchemaData(spark.read().schema(GRAPH_SCHEMA)
                .json(datacitePath));
        datacite.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/datacite");

        Dataset<Row> crossref = getSelectGraphSchemaData(spark.read().schema(GRAPH_SCHEMA).json(crossrefPath));
        crossref.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/crossref");

        Dataset<Row> pubmed = getSelectGraphSchemaData(spark.read().schema(GRAPH_SCHEMA).json(pubmedPath));
        pubmed.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/pubmed");

        Dataset<Row> oaire_entities =
                spark.createDataFrame(Collections.emptyList(), GRAPH_SCHEMA);
        for(EntityType entity: ModelSupport.entityTypes.keySet()) {
            if (ModelSupport.isResult(entity)) {
                oaire_entities = oaire_entities.union(spark.read().schema(GRAPH_SCHEMA).json(oairePath + "/" + entity.name()));

            }
        }
        Dataset<Row> oaire = getSelectGraphSchemaData(oaire_entities);
        oaire.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/oaire");


        //the output model for all the datasets will be:
        //id : the openaire identifier for the resource
        //authors.fullname the fullname of the author
        //authors.raw_affiliation_strings the list of raw_affiliation_strings associated to the author
        //todo decide if we want also to include authors.pid not from oalex but from the other sources
        Dataset<Row> oalex = getSelectOalexSchemaData(spark
                .read()
                .schema(OALEX_SCHEMA)
                .json(oalexPath));
        oalex.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/oalex");

        Dataset<Row> inputDataset = spark.createDataFrame(Collections.emptyList(), DATASET_SCHEMA);
        if(importIIS) {
            Dataset<Row> iis = getSelectIISData(spark.sql(IIS_QUERY)
                    .as(Encoders.bean(IISModel.class)));

            iis.write().mode(SaveMode.Overwrite).option("compression", "gzip").json(workingDir + "exploded/iis");
            inputDataset = iis;
        }
        Dataset<Row> publishers = getSelectPublisherSchemaData(spark.read().schema(PUBLISHER_SCHEMA).json(publishersPath));
        publishers.write().mode(SaveMode.Overwrite).option("compression","gzip").json(workingDir + "exploded/publishers");


        inputDataset = inputDataset.union(oalex).union(oaire).union(publishers)
                .union(crossref)
                .union(datacite)
                .union(pubmed)
                .distinct()
                ;


        inputDataset.select( col("raw_affiliation_string"))
                .distinct()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .json(workingDir + "/all_strings");

        Dataset<Row> alreadyMatched = spark.createDataFrame(Collections.emptyList(), AFFILIATION_SCHEMA);


        if (!startFromScratch){
            alreadyMatched = spark.read().schema(AFFILIATION_SCHEMA)
                    .json(oldMatches);
        }
        Dataset<Row> affStrings = spark.read().schema(AFFILIATION_STRING_SCHEMA).json(workingDir + "/all_strings");
        Dataset<Row> newToMatch =  affStrings.join(alreadyMatched, affStrings.col("raw_affiliation_string").equalTo(alreadyMatched.col("affiliation")), "left")
                .filter( col("affiliation").isNull())
                .select("raw_affiliation_string")
                .distinct();

        newToMatch.write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .json(workingDir+"/toMatch" );

    }

    private static Dataset<Row> getSelectPublisherSchemaData(Dataset<Row> dataset_entities) {
        return dataset_entities.filter( col("success").equalTo(true))
                .withColumn("authors", col("parsing_output.authors"))
                .select( col("id"), col("doi"),
                        explode(col("authors")).alias("author"))
                .withColumn("graphId" ,expr("selectId(doi, id)"))
                .drop(col("id"))
                .drop(col("doi"))
                .withColumn("fullname", col("author.name.full"))
                .withColumn("firstname", col("author.name.first"))
                .withColumn("lastname", col("author.name.last"))
                .withColumn("raw_affiliation_strings",  col("author.raw_affiliations"))
                .withColumn("corresponding", col("author.corresponding"))
                .withColumn("contributor_roles", col("author.contributor_roles"))
                .withColumn("pids", col("author.pids"))
                .drop(col("author"))
                .select(col("graphId"),col("fullname"), explode(col("raw_affiliation_strings")).alias("raw_affiliation_string")
                        ,col("corresponding"), col("contributor_roles"), col("pids"))
                .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .withColumn("id",  col("graphId"))
                .drop(col("graphId"))
                .select("id","fullname","firstname", "lastname", "raw_affiliation_string","corresponding","contributor_roles", "pids")
                .as(RowEncoder.apply(DATASET_SCHEMA));
    }

    private static Dataset<Row> getSelectGraphSchemaData(Dataset<Row> dataset_entities) {

        return dataset_entities
                .select(col("id"), explode(col("author")).alias("author"))
                .select(col("id"), col("author"), explode(col("author.rawAffiliationString")).alias("raw_affiliation_string"))
                .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .withColumn("fullname", col("author.fullName"))
                .withColumn("firstname", col("author.name"))
                .withColumn("lastname", col("author.surname"))
                .withColumn("pid", col("author.pid"))
                .drop("author")
                .withColumn("corresponding", lit(null))
                .withColumn("contributor_roles", lit(null))
                .withColumn(
                        "pids",
                        when(col("pid").isNotNull(),
                                transform(
                                        col("pid"),
                                        x -> struct(
                                                x.getField("value").alias("value"),
                                                x.getField("qualifier").getField("classid").alias("schema")
                                        )
                                )
                        ).otherwise(
                                array(struct(
                                        lit(null).cast("string").alias("value"),
                                        lit(null).cast("string").alias("schema")
                                )).cast("array<struct<value:string,schema:string>>")
                        )
                )
                .select(col("id"), col("fullname"), col("firstname"), col("lastname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"), col("pids"))

                .as(RowEncoder.apply(DATASET_SCHEMA));
    }

    private static Dataset<Row> getSelectOalexSchemaData(Dataset<Row> dataset_entities){
        return dataset_entities.filter(col("doi").isNotNull())
                .withColumn("id",  expr("md5HashWithPrefix(doi)"))
                .select(col("id"),
                        explode( col("authorships")).alias("author"))
                .withColumn("fullname", col("author.author.display_name"))
                .withColumn("firstname", lit(null))
                .withColumn("lastname", lit(null))
                .withColumn("raw_affiliation_strings", col("author.raw_affiliation_strings"))
                .select(col("id"), col("fullname"),
                        explode(col("raw_affiliation_strings")).alias("raw_affiliation_string"))
                .filter("raw_affiliation_string IS NOT NULL AND TRIM(raw_affiliation_string) != '' AND LOWER(raw_affiliation_string) NOT IN ('unknown', 'none')")
                .withColumn("corresponding", lit(null))
                .withColumn("contributor_roles", lit(null))
                .withColumn(
                        "pids",
                        lit(null).cast(PID_SCHEMA)
                )
                .select(col("id"), col("fullname"), col("firstname"), col("lastname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"), col("pids"))
                .as(RowEncoder.apply(DATASET_SCHEMA));
    }

    private static Dataset<Row> getSelectIISData(Dataset<IISModel> dataset_entities){
        return dataset_entities

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
                .withColumn(
                        "pids",
                        lit(null).cast(PID_SCHEMA)
                )
                .withColumn("firstname", lit(null))
                .withColumn("lastname", lit(null))
                .select(col("id"), col("fullname"), col("firstname"), col("lastname"), col("raw_affiliation_string"), col("corresponding"), col("contributor_roles"), col("pids"))
                .as(RowEncoder.apply(DATASET_SCHEMA));
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

