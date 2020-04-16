package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static eu.dnetlib.dhp.PropagationConstant.RELATION_RESULT_ORGANIZATION_REL_CLASS;
import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareResultInstRepoAssociation {

    private static final Logger log = LoggerFactory.getLogger(PrepareResultInstRepoAssociation.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception{

        String jsonConfiguration = IOUtils.toString(PrepareResultInstRepoAssociation.class
                .getResourceAsStream("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_prepareresultorg_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String datasourceOrganizationPath = parser.get("datasourceOrganizationPath");
        log.info("datasourceOrganizationPath {}: ", datasourceOrganizationPath);

        final String alreadyLinkedPath = parser.get("alreadyLinkedPath");
        log.info("alreadyLinkedPath {}: ", alreadyLinkedPath);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
                    readNeededResources(spark, inputPath);
                    prepareDatasourceOrganizationAssociations(spark, datasourceOrganizationPath, alreadyLinkedPath);
                    prepareAlreadyLinkedAssociation(spark, alreadyLinkedPath);
                });
    }

    private static void prepareAlreadyLinkedAssociation(SparkSession spark, String alreadyLinkedPath) {
        String query = "Select source resultId, collect_set(target) organizationSet " +
                "from relation " +
                "where datainfo.deletedbyinference = false " +
                "and relClass = '" + RELATION_RESULT_ORGANIZATION_REL_CLASS +"' " +
                "group by source";
        

        spark.sql(query)
                .as(Encoders.bean(ResultOrganizationSet.class))
                .toJavaRDD()
                .map(r -> OBJECT_MAPPER.writeValueAsString(r))
                .saveAsTextFile(alreadyLinkedPath, GzipCodec.class);
//                .as(Encoders.bean(ResultOrganizationSet.class))
//                .toJSON()
//                .write()
//                .mode(SaveMode.Overwrite)
//                .option("compression","gzip")
//                .text(alreadyLinkedPath);
    }

    private static void readNeededResources(SparkSession spark, String inputPath) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        org.apache.spark.sql.Dataset<Datasource> datasource = spark.createDataset(sc.textFile(inputPath + "/datasource")
                .map(item -> new ObjectMapper().readValue(item, Datasource.class)).rdd(), Encoders.bean(Datasource.class));

        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));

        org.apache.spark.sql.Dataset<Organization> organization = spark.createDataset(sc.textFile(inputPath + "/organization")
                .map(item -> new ObjectMapper().readValue(item, Organization.class)).rdd(), Encoders.bean(Organization.class));

        datasource.createOrReplaceTempView("datasource");
        relation.createOrReplaceTempView("relation");
        organization.createOrReplaceTempView("organization");
    }

    private static void prepareDatasourceOrganizationAssociations(SparkSession spark, String datasourceOrganizationPath,
                                                                  String alreadyLinkedPath){


        String query = "SELECT source datasourceId, target organizationId " +
                "FROM ( SELECT id " +
                "FROM datasource " +
                "WHERE datasourcetype.classid = 'pubsrepository::institutional' " +
                "AND datainfo.deletedbyinference = false  ) d " +
                "JOIN ( SELECT source, target " +
                "FROM relation " +
                "WHERE relclass = 'provides' " +
                "AND datainfo.deletedbyinference = false ) rel " +
                "ON d.id = rel.source ";

        spark.sql(query)
        .as(Encoders.bean(DatasourceOrganization.class))
        .toJSON()
        .write()
        .mode(SaveMode.Overwrite)
        .option("compression","gzip")
        .text(datasourceOrganizationPath);



    }

}
