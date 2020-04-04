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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Optional;

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

        RDD<JoinedEntity> joined = spark.read()
                .load(inputPath)
                .as(Encoders.kryo(EntityRelEntity.class))
                .javaRDD()
                .map(e -> getJoinedEntity(e))
                .mapToPair(e -> new Tuple2<>(e.getEntity().getId(), e))
                .reduceByKey((j1, j2) -> getJoinedEntity(j1, j2))
                .map(Tuple2::_2)
                .rdd();

        spark.createDataset(joined, Encoders.bean(JoinedEntity.class))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);

    }

    private static JoinedEntity getJoinedEntity(JoinedEntity j1, JoinedEntity j2) {
        JoinedEntity je = new JoinedEntity();
        je.setEntity(je.getEntity());
        je.setType(j1.getType());

        Links links = new Links();
        links.addAll(j1.getLinks());
        links.addAll(j2.getLinks());

        return je;
    }

    private static JoinedEntity getJoinedEntity(EntityRelEntity e) {
        JoinedEntity j = new JoinedEntity();
        j.setEntity(toOafEntity(e.getEntity()));
        j.setType(EntityType.valueOf(e.getEntity().getType()));
        Links links = new Links();
        links.add(new eu.dnetlib.dhp.oa.provision.model.Tuple2(e.getRelation(), e.getTarget()));
        j.setLinks(links);
        return j;
    }

    private static OafEntity toOafEntity(TypedRow typedRow) {
        return parseOaf(typedRow.getOaf(), typedRow.getType());
    }

    private static OafEntity parseOaf(final String json, final String type) {
        try {
            switch (GraphMappingUtils.EntityType.valueOf(type)) {
                case publication:
                    return OBJECT_MAPPER.readValue(json, Publication.class);
                case dataset:
                    return OBJECT_MAPPER.readValue(json, Dataset.class);
                case otherresearchproduct:
                    return OBJECT_MAPPER.readValue(json, OtherResearchProduct.class);
                case software:
                    return OBJECT_MAPPER.readValue(json, Software.class);
                case datasource:
                    return OBJECT_MAPPER.readValue(json, Datasource.class);
                case organization:
                    return OBJECT_MAPPER.readValue(json, Organization.class);
                case project:
                    return OBJECT_MAPPER.readValue(json, Project.class);
                default:
                    throw new IllegalArgumentException("invalid type: " + type);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

}
