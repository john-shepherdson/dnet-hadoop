package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.io.Text;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToOrganizationFromIstRepoJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToOrganizationFromIstRepoJob.class.getResourceAsStream("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_resulttoorganizationfrominstrepo_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToOrganizationFromIstRepoJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/resulttoorganizationfrominstitutionalrepositories";

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        //get the institutional repositories
        JavaPairRDD<String, TypedRow> datasources = sc.sequenceFile(inputPath + "/datasource", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Datasource.class))
                .filter(ds -> INSTITUTIONAL_REPO_TYPE.equals(ds.getDatasourcetype().getClassid()))
                .map(ds -> new TypedRow().setSourceId(ds.getId()))
                .mapToPair(toPair());


        JavaPairRDD<String, TypedRow> rel_datasource_organization = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class))
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> RELATION_DATASOURCEORGANIZATION_REL_TYPE.equals(r.getRelClass()) && RELATION_DATASOURCE_ORGANIZATION_REL_CLASS.equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> instdatasource_organization = datasources.join(rel_datasource_organization)
                .map(x -> x._2()._2())
                .mapToPair(toPair());

        JavaRDD<Relation> relations = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class));
        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class));
        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class));
        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class));
        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class));

        JavaPairRDD<String, TypedRow> datasource_results = publications
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                })
                .union(datasets
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(software
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(other
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }));

        JavaRDD<Relation> newRels = instdatasource_organization.join(datasource_results)
                .flatMap(c -> {
                    List<Relation> rels = new ArrayList();
                    rels.add(getRelation(c._2()._1().getTargetId(), c._2()._2().getTargetId(), RELATION_ORGANIZATION_RESULT_REL_CLASS,
                            RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                    rels.add(getRelation(c._2()._2().getTargetId(), c._2()._1().getTargetId(), RELATION_ORGANIZATION_RESULT_REL_CLASS,
                            RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                    return rels.iterator();
                });
        newRels.map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation_new");

        newRels.union(relations).map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation");
    }


}
