package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.io.Text;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToOrganizationFromIstRepoJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToOrganizationFromIstRepoJob.class.getResourceAsStream("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_propagationresulaffiliationfrominstrepo_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToOrganizationFromIstRepoJob.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/resulttoorganizationfrominstitutionalrepositories";

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        org.apache.spark.sql.Dataset<Datasource> datasource = spark.createDataset(sc.textFile(inputPath + "/datasource")
                .map(item -> new ObjectMapper().readValue(item, Datasource.class)).rdd(), Encoders.bean(Datasource.class));

        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));

        org.apache.spark.sql.Dataset<Organization> organization = spark.createDataset(sc.textFile(inputPath + "/organziation")
                .map(item -> new ObjectMapper().readValue(item, Organization.class)).rdd(), Encoders.bean(Organization.class));

        datasource.createOrReplaceTempView("datasource");
        relation.createOrReplaceTempView("relation");
        organization.createOrReplaceTempView("organization");

        String query = "SELECT source ds, target org " +
                "FROM ( SELECT id " +
                "FROM datasource " +
                "WHERE datasourcetype.classid = 'pubsrepository::institutional' " +
                "AND datainfo.deletedbyinference = false  ) d " +
                "JOIN ( SELECT source, target " +
                "FROM relation " +
                "WHERE relclass = 'provides' " +
                "AND datainfo.deletedbyinference = false ) rel " +
                "ON d.id = rel.source ";

        org.apache.spark.sql.Dataset<Row> rels = spark.sql(query);
        rels.createOrReplaceTempView("rels");

        org.apache.spark.sql.Dataset<Dataset> dataset = spark.createDataset(sc.textFile(inputPath + "/dataset")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));

        org.apache.spark.sql.Dataset<OtherResearchProduct> other = spark.createDataset(sc.textFile(inputPath + "/otherresearchproduct")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));

        org.apache.spark.sql.Dataset<Software> software = spark.createDataset(sc.textFile(inputPath + "/software")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Software.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));

        org.apache.spark.sql.Dataset<Publication> publication = spark.createDataset(sc.textFile(inputPath + "/publication")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Publication.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));


        software.createOrReplaceTempView("software");
        final JavaRDD<Row> toupdateresultsoftware = propagateOnResult(spark, "software");

        dataset.createOrReplaceTempView("dataset");
        final JavaRDD<Row> toupdateresultdataset = propagateOnResult(spark, "dataset");

        other.createOrReplaceTempView("other");
        final JavaRDD<Row> toupdateresultother = propagateOnResult(spark, "other");

        publication.createOrReplaceTempView("publication");
        final JavaRDD<Row> toupdateresultpublication = propagateOnResult(spark, "publication");

        writeUpdates(toupdateresultsoftware, toupdateresultdataset, toupdateresultother, toupdateresultpublication, outputPath);
        
    }

    private static void writeUpdates(JavaRDD<Row> toupdateresultsoftware, JavaRDD<Row> toupdateresultdataset, JavaRDD<Row> toupdateresultother, JavaRDD<Row> toupdateresultpublication, String outputPath) {
        createUpdateForRelationWrite(toupdateresultsoftware, outputPath, "update_software");
        createUpdateForRelationWrite(toupdateresultdataset, outputPath, "update_dataset");
        createUpdateForRelationWrite(toupdateresultother, outputPath, "update_other");
        createUpdateForRelationWrite(toupdateresultpublication, outputPath, "update_publication");

    }

    private static void createUpdateForRelationWrite(JavaRDD<Row> toupdaterelation, String outputPath, String update_type) {
        toupdaterelation.flatMap(s -> {
            List<Relation> relationList = new ArrayList<>();
            List<String> orgs = s.getList(1);
            String resId = s.getString(0);
            for (String org : orgs) {
                relationList.add(getRelation(org, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                relationList.add(getRelation(resId, org, RELATION_RESULT_ORGANIZATION_REL_CLASS,
                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));

            }
            return relationList.iterator();
        }).map(s -> new ObjectMapper().writeValueAsString(s)).saveAsTextFile(outputPath + "/" + update_type);
    }

    private static JavaRDD<Row> propagateOnResult(SparkSession spark, String table) {
        String query;
        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
                "FROM ( SELECT id, instance " +
                "FROM " + table +
                " WHERE datainfo.deletedbyinference = false)  ds " +
                "LATERAL VIEW EXPLODE(instance) i AS inst";
        org.apache.spark.sql.Dataset<Row> cfhb = spark.sql(query);
        cfhb.createOrReplaceTempView("cfhb");

        return organizationPropagationAssoc(spark, "cfhb").toJavaRDD();

    }

    private static org.apache.spark.sql.Dataset<Row> organizationPropagationAssoc(SparkSession spark, String cfhbTable){
        String  query = "SELECT id, collect_set(org) org "+
                "FROM ( SELECT id, org " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , org     " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

}
