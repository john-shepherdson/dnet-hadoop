package eu.dnetlib.dhp.graph;

import com.google.common.collect.Sets;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityPayload;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.spark.sql.Encoders.bean;

public class SparkGraphIndexingJob {

    private final static String ENTITY_NODES_PATH = "/tmp/entity_node";
    private static final long LIMIT = 100;

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphIndexingJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphIndexingJob.class.getSimpleName())
                .master(parser.get("master"))
                .config("hive.metastore.uris", parser.get("hive_metastore_uris"))
                .config("spark.driver.cores", 1)
                .config("spark.executor.cores", 1)
                .config("spark.yarn.executor.memoryOverhead", "4G")
                .config("spark.yarn.driver.memoryOverhead", "4G")
                .enableHiveSupport()
                .getOrCreate();

        final String hiveDbName = parser.get("hive_db_name");

        final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(new Path(ENTITY_NODES_PATH))) {
            fs.delete(new Path(ENTITY_NODES_PATH), true);
        }

        spark
            .sql(getJoinEntitiesSQL(hiveDbName))
            .transform(toEntityNode())
            /*
            .map((MapFunction<EntityNode, String>) r -> {
                return null;
            }, bean(String.class))
            */
            .rdd()

            .saveAsTextFile(ENTITY_NODES_PATH, GzipCodec.class);
    }

    private static AbstractFunction1<Dataset<Row>, Dataset<EntityNode>> toEntityNode() {
        return new AbstractFunction1<Dataset<Row>, Dataset<EntityNode>>() {
            @Override
            public Dataset<EntityNode> apply(Dataset<Row> d) {
                return d.map((MapFunction<Row, EntityNode>) r -> {

                    final List<String> res = r.getList(r.fieldIndex("related_entity"));
                    final byte[] payload = r.getAs("payload");
                    return new EntityNode(r.getAs("id"), r.getAs("type"), new String(payload))
                            .setRelatedEntities(res
                                    .stream()
                                    .map(re -> new Tuple2<>(substringBefore(re, "@@"), substringAfter(re, "@@")))
                                    .map(re -> new RelatedEntity(r.getAs("reltype"), r.getAs("subreltype"), r.getAs("relclass"), re._1(), re._2()))
                                    .limit(LIMIT)
                                    .collect(Collectors.toList()));

                }, bean(EntityNode.class));
            }
        };
    }

    private static String getJoinEntitiesSQL(String hiveDbName) {
        return String.format(
                "SELECT " +
                        "E_s.id AS id, " +
                        "E_s.type AS type, " +
                        "E_s.payload AS payload, " +
                        "r.reltype AS reltype, r.subreltype AS subreltype, r.relclass AS relclass, " +
                        "collect_list(concat(E_t.type, '@@', E_t.payload)) AS related_entity " +
                "FROM %s.entities " + "" /*"TABLESAMPLE(0.1 PERCENT) "*/ + "E_s " +
                        "LEFT JOIN %s.relation r ON (r.source = E_s.id) " +
                        "JOIN %s.entities E_t ON (E_t.id = r.target) \n" +
                "GROUP BY E_s.id, E_s.type, E_s.payload, r.reltype, r.subreltype, r.relclass", hiveDbName, hiveDbName, hiveDbName);
    }

}
