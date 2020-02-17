package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.actionmanager.PromoteActionSetFromHDFSFunctions.*;
import static org.apache.spark.sql.functions.*;

public class PromoteActionSetFromHDFSJob {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(
                PromoteActionSetFromHDFSJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/actionmanager/actionmanager_input_parameters.json")));
        parser.parseArgument(args);

        String inputGraphPath = parser.get("inputGraphPath");
        List<String> inputActionSetPaths = Arrays.asList(parser.get("inputActionSetPaths").split(","));
        String outputGraphPath = parser.get("outputGraphPath");

        SparkConf conf = new SparkConf();
        conf.setMaster(parser.get("master"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {

            // ----- READ -----
            // dataset
            Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> datasetDS = readGraphTable(
                    spark, String.format("%s/dataset", inputGraphPath), eu.dnetlib.dhp.schema.oaf.Dataset.class)
                    .cache();
            datasetDS.printSchema();
            datasetDS.show();

            // datasource
            Dataset<Datasource> datasourceDS =
                    readGraphTable(spark, String.format("%s/datasource", inputGraphPath), Datasource.class)
                            .cache();
            datasourceDS.printSchema();
            datasourceDS.show();

            // organization
            Dataset<Organization> organizationDS =
                    readGraphTable(spark, String.format("%s/organization", inputGraphPath), Organization.class)
                            .cache();
            organizationDS.printSchema();
            organizationDS.show();

            // otherresearchproduct
            Dataset<OtherResearchProduct> otherResearchProductDS =
                    readGraphTable(spark, String.format("%s/otherresearchproduct", inputGraphPath), OtherResearchProduct.class)
                            .cache();
            otherResearchProductDS.printSchema();
            otherResearchProductDS.show();

            // project
            Dataset<Project> projectDS =
                    readGraphTable(spark, String.format("%s/project", inputGraphPath), Project.class)
                            .cache();
            projectDS.printSchema();
            projectDS.show();

            // publication
            Dataset<Publication> publicationDS =
                    readGraphTable(spark, String.format("%s/publication", inputGraphPath), Publication.class)
                            .cache();
            publicationDS.printSchema();
            publicationDS.show();

            // relation
            Dataset<Relation> relationDS =
                    readGraphTable(spark, String.format("%s/relation", inputGraphPath), Relation.class)
                            .cache();
            relationDS.printSchema();
            relationDS.show();

            // software
            Dataset<Software> softwareDS =
                    readGraphTable(spark, String.format("%s/software", inputGraphPath), Software.class)
                            .cache();
            softwareDS.printSchema();
            softwareDS.show();

            // actions
            Dataset<String> actionPayloadDS = inputActionSetPaths.stream()
                    .map(inputActionSetPath -> readActionSetPayload(spark, inputActionSetPath))
                    .reduce(Dataset::union)
                    .get()
                    .cache();
            actionPayloadDS.printSchema();
            actionPayloadDS.show();
            System.out.println(String.join("\n", actionPayloadDS.takeAsList(20)));

            Dataset<String> relationActionPayloadDS = filterActionPayloadForRelations(actionPayloadDS)
                    .cache();
            relationActionPayloadDS.printSchema();
            relationActionPayloadDS.show();

            Dataset<String> entityActionPayloadDS = filterActionPayloadForEntity(actionPayloadDS)
                    .cache();
            entityActionPayloadDS.printSchema();
            entityActionPayloadDS.show();

            // ----- LOGIC -----
            Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> processedDatasetDS =
                    processEntityDS(datasetDS, entityActionPayloadDS, eu.dnetlib.dhp.schema.oaf.Dataset.class);
            Dataset<Datasource> processedDatasourceDS =
                    processEntityDS(datasourceDS, entityActionPayloadDS, Datasource.class);
            Dataset<Organization> processedOrganizationDS =
                    processEntityDS(organizationDS, entityActionPayloadDS, Organization.class);
            Dataset<OtherResearchProduct> processedOtherResearchProductDS =
                    processEntityDS(otherResearchProductDS, entityActionPayloadDS, OtherResearchProduct.class);
            Dataset<Project> processedProjectDS =
                    processEntityDS(projectDS, entityActionPayloadDS, Project.class);
            Dataset<Publication> processedPublicationDS =
                    processEntityDS(publicationDS, entityActionPayloadDS, Publication.class);
            Dataset<Relation> processedRelationDS =
                    processRelationDS(relationDS, relationActionPayloadDS);
            Dataset<Software> processedSoftwareDS =
                    processEntityDS(softwareDS, entityActionPayloadDS, Software.class);

            // ----- SAVE -----
            processedDatasetDS.write()
                    .save(String.format("%s/dataset", outputGraphPath));
            processedDatasourceDS.write()
                    .save(String.format("%s/datasource", outputGraphPath));
            processedOrganizationDS.write()
                    .save(String.format("%s/organization", outputGraphPath));
            processedOtherResearchProductDS.write()
                    .save(String.format("%s/otherresearchproduct", outputGraphPath));
            processedProjectDS.write()
                    .save(String.format("%s/project", outputGraphPath));
            processedPublicationDS.write()
                    .save(String.format("%s/publication", outputGraphPath));
            processedRelationDS.write()
                    .save(String.format("%s/relation", outputGraphPath));
            processedSoftwareDS.write()
                    .save(String.format("%s/software", outputGraphPath));
        }
    }

    private static final StructType KV_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("key", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())
            ));

    private static <T extends Oaf> Dataset<T> readGraphTable(SparkSession spark, String path, Class<T> clazz) {
        JavaRDD<Row> rows = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(path, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        return spark.createDataFrame(rows, KV_SCHEMA)
                .map((MapFunction<Row, T>) row -> new ObjectMapper().readValue(row.<String>getAs("value"), clazz),
                        Encoders.bean(clazz));
    }

    private static Dataset<String> readActionSetPayload(SparkSession spark, String inputActionSetPath) {
        JavaRDD<Row> actionsRDD = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(inputActionSetPath, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        return spark.createDataFrame(actionsRDD, KV_SCHEMA)
                .select(unbase64(get_json_object(col("value"), "$.TargetValue"))
                        .cast(DataTypes.StringType).as("target_value_json"))
                .as(Encoders.STRING());
    }

    private static Dataset<String> filterActionPayloadForRelations(Dataset<String> actionPayloadDS) {
        return actionPayloadDS
                .where(get_json_object(col("target_value_json"), "$.kind").equalTo("relation"));
    }

    private static Dataset<String> filterActionPayloadForEntity(Dataset<String> actionPayloadDS) {
        return actionPayloadDS
                .where(get_json_object(col("target_value_json"), "$.kind").equalTo("entity"));
    }


    private static <T extends OafEntity> Dataset<T> processEntityDS(Dataset<T> entityDS,
                                                                    Dataset<String> actionPayloadDS,
                                                                    Class<T> clazz) {
        Dataset<T> groupedAndMerged = groupEntitiesByIdAndMerge(entityDS, clazz);
        Dataset<T> joinedAndMerged = joinEntitiesWithActionPayloadAndMerge(groupedAndMerged,
                actionPayloadDS,
                PromoteActionSetFromHDFSJob::entityToActionPayloadJoinExpr,
                PromoteActionSetFromHDFSJob::actionPayloadToEntity,
                clazz);
        return groupEntitiesByIdAndMerge(joinedAndMerged, clazz);
    }

    private static <T extends OafEntity> Column entityToActionPayloadJoinExpr(Dataset<T> left,
                                                                              Dataset<String> right) {
        return left.col("id").equalTo(
                get_json_object(right.col("target_value_json"), "$.entity.id"));
    }

    public static <T extends OafEntity> T actionPayloadToEntity(String actionPayload,
                                                                Class<T> clazz) {
        try {
            OafProtos.Oaf oldEntity = new ObjectMapper().readValue(actionPayload, OafProtos.Oaf.class);
            return entityOldToNew(oldEntity, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO
    private static <T extends OafEntity> T entityOldToNew(OafProtos.Oaf old,
                                                          Class<T> clazz) {
        return null;
    }

    //TODO
    private static Dataset<Relation> processRelationDS(Dataset<Relation> relationDS,
                                                       Dataset<String> actionPayloadDS) {
        return null;
    }
}
