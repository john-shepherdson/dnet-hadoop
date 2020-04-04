package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.EntityRelEntity;
import eu.dnetlib.dhp.oa.provision.model.SortableRelation;
import eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

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
 *  2) CreateRelatedEntitiesJob_phase1:
 *      prepare tuples [relation - target entity] (R - T):
 *      for each entity type E_i
 *          join (R.target = E_i.id),
 *          map E_i as RelatedEntity T_i, extracting only the necessary information beforehand to produce [R - T_i]
 *          save the tuples [R - T_i] in append mode
 *
 *  3) CreateRelatedEntitiesJob_phase2:
 *      prepare tuples [source entity - relation - target entity] (S - R - T):
 *      create the union of the each entity type, hash by id (S)
 *      for each [R - T_i] produced in phase1
 *          join S.id = [R - T_i].source to produce (S_i - R - T_i)
 *          save in append mode
 *
 *  4) AdjacencyListBuilderJob:
 *      given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mappnig the result as JoinedEntity
 *
 *  5) XmlConverterJob:
 *      convert the JoinedEntities as XML records
 */
public class CreateRelatedEntitiesJob_phase1 {

    private static final Logger log = LoggerFactory.getLogger(CreateRelatedEntitiesJob_phase1.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils.toString(
                PrepareRelationsJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_related_entities_pahase1.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputRelationsPath = parser.get("inputRelationsPath");
        log.info("inputRelationsPath: {}", inputRelationsPath);

        String inputEntityPath = parser.get("inputEntityPath");
        log.info("inputEntityPath: {}", inputEntityPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        String graphTableClassName = parser.get("graphTableClassName");
        log.info("graphTableClassName: {}", graphTableClassName);

        Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(getKryoClasses());

        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    joinRelationEntity(spark, inputRelationsPath, inputEntityPath, entityClazz, outputPath);
                });
    }

    private static <E extends OafEntity> void joinRelationEntity(SparkSession spark, String inputRelationsPath, String inputEntityPath, Class<E> entityClazz, String outputPath) {

        Dataset<Tuple2<String, SortableRelation>> relsByTarget = readPathRelation(spark, inputRelationsPath)
                .map((MapFunction<SortableRelation, Tuple2<String, SortableRelation>>) r -> new Tuple2<>(r.getTarget(), r),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(SortableRelation.class)));

        Dataset<Tuple2<String, E>> entities = readPathEntity(spark, inputEntityPath, entityClazz)
                .map((MapFunction<E, Tuple2<String, E>>) e -> new Tuple2<>(e.getId(), e),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(entityClazz)))
                .cache();

        relsByTarget
                .joinWith(entities, entities.col("_1").equalTo(relsByTarget.col("_1")), "inner")
                .filter((FilterFunction<Tuple2<Tuple2<String, SortableRelation>, Tuple2<String, E>>>)
                        value -> value._2()._2().getDataInfo().getDeletedbyinference() == false)
                .map((MapFunction<Tuple2<Tuple2<String, SortableRelation>, Tuple2<String, E>>, EntityRelEntity>)
                        t -> new EntityRelEntity(t._1()._2(), GraphMappingUtils.asRelatedEntity(t._2()._2(), entityClazz)),
                        Encoders.bean(EntityRelEntity.class))
                .write()
                .mode(SaveMode.Append)
                .parquet(outputPath);
    }

    private static <E extends OafEntity> Dataset<E> readPathEntity(SparkSession spark, String inputEntityPath, Class<E> entityClazz) {

        log.info("Reading Graph table from: {}", inputEntityPath);
        return spark
                .read()
                .textFile(inputEntityPath)
                .map((MapFunction<String, E>) value -> OBJECT_MAPPER.readValue(value, entityClazz), Encoders.bean(entityClazz));
    }

    /**
     * Reads a Dataset of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text file,
     *
     * @param spark
     * @param relationPath
     * @return the Dataset<SortableRelation> containing all the relationships
     */
    private static Dataset<SortableRelation> readPathRelation(SparkSession spark, final String relationPath) {

        log.info("Reading relations from: {}", relationPath);
        return spark.read()
                .load(relationPath)
                .as(Encoders.bean(SortableRelation.class));
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }


}
