package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.EntityRelEntity;
import eu.dnetlib.dhp.oa.provision.model.TypedRow;
import eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
public class CreateRelatedEntitiesJob_phase2 {

    private static final Logger log = LoggerFactory.getLogger(CreateRelatedEntitiesJob_phase2.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils.toString(
                PrepareRelationsJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_related_entities_pahase2.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputRelatedEntitiesPath = parser.get("inputRelatedEntitiesPath");
        log.info("inputRelatedEntitiesPath: {}", inputRelatedEntitiesPath);

        String inputGraphRootPath = parser.get("inputGraphRootPath");
        log.info("inputGraphRootPath: {}", inputGraphRootPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        int numPartitions = Integer.parseInt(parser.get("numPartitions"));
        log.info("numPartitions: {}", numPartitions);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(getKryoClasses());

        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    joinAllEntities(spark, inputRelatedEntitiesPath, inputGraphRootPath, outputPath, numPartitions);
                });
    }

    private static void joinAllEntities(SparkSession spark, String inputRelatedEntitiesPath, String inputGraphRootPath, String outputPath, int numPartitions) {

        Dataset<Tuple2<String, TypedRow>> entities = readAllEntities(spark, inputGraphRootPath, numPartitions);
        Dataset<Tuple2<String, EntityRelEntity>> relsBySource = readRelatedEntities(spark, inputRelatedEntitiesPath);

        entities
                .joinWith(relsBySource, entities.col("_1").equalTo(relsBySource.col("_1")), "left_outer")
                .map((MapFunction<Tuple2<Tuple2<String, TypedRow>, Tuple2<String, EntityRelEntity>>, EntityRelEntity>) value -> {
                    EntityRelEntity re = new EntityRelEntity();
                    re.setEntity(value._1()._2());
                    Optional<EntityRelEntity> related = Optional.ofNullable(value._2()).map(Tuple2::_2);
                    if (related.isPresent()) {
                        re.setRelation(related.get().getRelation());
                        re.setTarget(related.get().getTarget());
                    }
                    return re;
                }, Encoders.bean(EntityRelEntity.class))
                .repartition(numPartitions)
                .filter((FilterFunction<EntityRelEntity>) value -> value.getEntity() != null && StringUtils.isNotBlank(value.getEntity().getId()))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);
    }

    private static Dataset<Tuple2<String, TypedRow>> readAllEntities(SparkSession spark, String inputGraphPath, int numPartitions) {
        Dataset<TypedRow> publication = readPathEntity(spark, inputGraphPath + "/publication", Publication.class);
        Dataset<TypedRow> dataset = readPathEntity(spark, inputGraphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);
        Dataset<TypedRow> other = readPathEntity(spark, inputGraphPath + "/otherresearchproduct", OtherResearchProduct.class);
        Dataset<TypedRow> software = readPathEntity(spark, inputGraphPath + "/software", Software.class);
        Dataset<TypedRow> datasource = readPathEntity(spark, inputGraphPath + "/datasource", Datasource.class);
        Dataset<TypedRow> organization = readPathEntity(spark, inputGraphPath + "/organization", Organization.class);
        Dataset<TypedRow> project = readPathEntity(spark, inputGraphPath + "/project", Project.class);

        return publication
                .union(dataset)
                .union(other)
                .union(software)
                .union(datasource)
                .union(organization)
                .union(project)
                .map((MapFunction<TypedRow, Tuple2<String, TypedRow>>)
                                value -> new Tuple2<>(value.getId(), value),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(TypedRow.class)))
                .repartition(numPartitions);
    }

    private static Dataset<Tuple2<String, EntityRelEntity>> readRelatedEntities(SparkSession spark, String inputRelatedEntitiesPath) {

        log.info("Reading related entities from: {}", inputRelatedEntitiesPath);

        final List<String> paths = HdfsSupport.listFiles(inputRelatedEntitiesPath, spark.sparkContext().hadoopConfiguration());

        log.info("Found paths: {}", String.join(",", paths));

        return spark.read()
                .load(toSeq(paths))
                .as(Encoders.bean(EntityRelEntity.class))
                .map((MapFunction<EntityRelEntity, Tuple2<String, EntityRelEntity>>)
                                value -> new Tuple2<>(value.getRelation().getSource(), value),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(EntityRelEntity.class)));
    }

    private static <E extends OafEntity> Dataset<TypedRow> readPathEntity(SparkSession spark, String inputEntityPath, Class<E> entityClazz) {

        log.info("Reading Graph table from: {}", inputEntityPath);
        return spark
                .read()
                .textFile(inputEntityPath)
                .map((MapFunction<String, E>) value -> OBJECT_MAPPER.readValue(value, entityClazz), Encoders.bean(entityClazz))
                .map((MapFunction<E, TypedRow>) value -> getTypedRow(StringUtils.substringAfterLast(inputEntityPath, "/"), value), Encoders.bean(TypedRow.class));
    }

    private static TypedRow getTypedRow(String type, OafEntity entity) throws JsonProcessingException {
        TypedRow t = new TypedRow();
        t.setType(type);
        t.setDeleted(entity.getDataInfo().getDeletedbyinference());
        t.setId(entity.getId());
        t.setOaf(OBJECT_MAPPER.writeValueAsString(entity));
        return t;
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

    private static Seq<String> toSeq(List<String> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

}
