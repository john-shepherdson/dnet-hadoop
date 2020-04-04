package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.SortableRelation;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

/**
 * Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects.
 * The operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization,
 * and all the possible relationships (similarity links produced by the Dedup process are excluded).
 *
 * The operation is implemented by sequentially joining one entity type at time (E) with the relationships (R), and again
 * by E, finally grouped by E.id;
 *
 * The workflow is organized in different parts aimed to to reduce the complexity of the operation
 *  1) PrepareRelationsJob:
 *      only consider relationships that are not virtually deleted ($.dataInfo.deletedbyinference == false), each entity
 *      can be linked at most to 100 other objects
 *
 *  2) JoinRelationEntityByTargetJob:
 *      prepare tuples [source entity - relation - target entity] (S - R - T):
 *      for each entity type E_i
 *          join (R.target = E_i.id),
 *          map E_i as RelatedEntity T_i, extracting only the necessary information beforehand to produce [R - T_i]
 *          join (E_i.id = [R - T_i].source), where E_i becomes the source entity S
 *
 *  3) AdjacencyListBuilderJob:
 *      given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mappnig the result as JoinedEntity
 *
 *  4) XmlConverterJob:
 *      convert the JoinedEntities as XML records
 */
public class PrepareRelationsJob {

    private static final Logger log = LoggerFactory.getLogger(PrepareRelationsJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int MAX_RELS = 100;

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils.toString(
                PrepareRelationsJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_prepare_relations.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputRelationsPath = parser.get("inputRelationsPath");
        log.info("inputRelationsPath: {}", inputRelationsPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    prepareRelationsFromPaths(spark, inputRelationsPath, outputPath);
                });
    }

    private static void prepareRelationsFromPaths(SparkSession spark, String inputRelationsPath, String outputPath) {
        RDD<SortableRelation> rels = readPathRelation(spark, inputRelationsPath)
                .filter((FilterFunction<SortableRelation>) r -> r.getDataInfo().getDeletedbyinference() == false)
                .javaRDD()
                .mapToPair((PairFunction<SortableRelation, String, List<SortableRelation>>) rel -> new Tuple2<>(
                        rel.getSource(),
                        Lists.newArrayList(rel)))
                .reduceByKey((v1, v2) -> {
                    v1.addAll(v2);
                    v1.sort(SortableRelation::compareTo);
                    if (v1.size() > MAX_RELS) {
                        return v1.subList(0, MAX_RELS);
                    }
                    return new ArrayList<>(v1.subList(0, MAX_RELS));
                })
                .flatMap(r -> r._2().iterator())
                .rdd();

        spark.createDataset(rels, Encoders.bean(SortableRelation.class))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);
    }

    /**
     * Reads a Dataset of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text file,
     *
     * @param spark
     * @param inputPath
     * @return the Dataset<SortableRelation> containing all the relationships
     */
    private static Dataset<SortableRelation> readPathRelation(SparkSession spark, final String inputPath) {
        return spark.read()
                .textFile(inputPath)
                .map((MapFunction<String, SortableRelation>) s -> OBJECT_MAPPER.readValue(s, SortableRelation.class),
                        Encoders.bean(SortableRelation.class));
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

}
