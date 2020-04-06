package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils.*;

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
public class AdjacencyListBuilderJob {

    private static final Logger log = LoggerFactory.getLogger(AdjacencyListBuilderJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final int MAX_LINKS = 100;

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        AdjacencyListBuilderJob.class
                                .getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_build_adjacency_lists.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("inputPath");
        log.info("inputPath: {}", inputPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(getKryoClasses());

        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    createAdjacencyLists(spark, inputPath, outputPath);
                });

    }

    private static void createAdjacencyLists(SparkSession spark, String inputPath, String outputPath) {

        log.info("Reading joined entities from: {}", inputPath);
        spark.read()
                .load(inputPath)
                .as(Encoders.bean(EntityRelEntity.class))
                .groupByKey((MapFunction<EntityRelEntity, String>) value -> value.getEntity().getId(), Encoders.STRING())
                .mapGroups((MapGroupsFunction<String, EntityRelEntity, JoinedEntity>) (key, values) -> {
                    JoinedEntity j = new JoinedEntity();
                    Links links = new Links();
                    while (values.hasNext() && links.size() < MAX_LINKS) {
                        EntityRelEntity curr = values.next();
                        if (j.getEntity() == null) {
                            j.setEntity(curr.getEntity());
                        }
                        links.add(new Tuple2(curr.getRelation(), curr.getTarget()));
                    }
                    j.setLinks(links);
                    return j;
                }, Encoders.bean(JoinedEntity.class))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

}
