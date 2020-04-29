package eu.dnetlib.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * This Spark class creates similarity relations between entities, saving result
 *
 * <p>param request: sourcePath entityType target Path
 */
public class SparkCreateSimRels {

  public static void main(String[] args) throws Exception {
    final ArgumentApplicationParser parser =
        new ArgumentApplicationParser(
            IOUtils.toString(
                SparkCreateSimRels.class.getResourceAsStream(
                    "/eu/dnetlib/dhp/sx/dedup/dedup_parameters.json")));
    parser.parseArgument(args);
    final SparkSession spark =
        SparkSession.builder()
            .appName(SparkCreateSimRels.class.getSimpleName())
            .master(parser.get("master"))
            .getOrCreate();

    final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    final String inputPath = parser.get("sourcePath");
    final String entity = parser.get("entity");
    final String targetPath = parser.get("targetPath");
    // final DedupConfig dedupConf =
    // DedupConfig.load(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));
    final DedupConfig dedupConf = DedupConfig.load(parser.get("dedupConf"));

    JavaPairRDD<String, MapDocument> mapDocument =
        sc.textFile(inputPath + "/" + entity)
            .mapToPair(
                s -> {
                  MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
                  return new Tuple2<>(d.getIdentifier(), d);
                });

    // create blocks for deduplication
    JavaPairRDD<String, List<MapDocument>> blocks =
        Deduper.createsortedBlocks(sc, mapDocument, dedupConf);
    // JavaPairRDD<String, Iterable<MapDocument>> blocks = Deduper.createBlocks(sc,
    // mapDocument, dedupConf);

    // create relations by comparing only elements in the same group
    final JavaPairRDD<String, String> dedupRels = Deduper.computeRelations2(sc, blocks, dedupConf);
    // final JavaPairRDD<String,String> dedupRels = Deduper.computeRelations(sc, blocks,
    // dedupConf);

    final JavaRDD<Relation> isSimilarToRDD =
        dedupRels.map(
            simRel -> {
              final Relation r = new Relation();
              r.setSource(simRel._1());
              r.setTarget(simRel._2());
              r.setRelClass("isSimilarTo");
              return r;
            });

    spark
        .createDataset(isSimilarToRDD.rdd(), Encoders.bean(Relation.class))
        .write()
        .mode("overwrite")
        .save(DedupUtility.createSimRelPath(targetPath, entity));
  }
}
