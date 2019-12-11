package eu.dnetlib.dedup;

import eu.dnetlib.dedup.graph.ConnectedComponent;
import eu.dnetlib.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;


/**
 * This Spark class creates similarity relations between entities, saving result
 *
 * param request:
 *  sourcePath
 *  entityType
 *  target Path
 */
public class SparkCreateSimRels {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String entity = parser.get("entity");
        final String targetPath = parser.get("targetPath");
//        final DedupConfig dedupConf = DedupConfig.load(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf2.json")));
        final DedupConfig dedupConf = DedupConfig.load(parser.get("dedupConf"));


        final long total = sc.textFile(inputPath + "/" + entity).count();

        JavaPairRDD<Object, MapDocument> vertexes = sc.textFile(inputPath + "/" + entity)
                .map(s->{
                    MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf,s);
                    return new Tuple2<>(d.getIdentifier(), d);})
                .mapToPair((PairFunction<Tuple2<String, MapDocument>, Object, MapDocument>) t -> new Tuple2<Object, MapDocument>((long) t._1().hashCode(), t._2()));




        JavaPairRDD<String, MapDocument> mapDocument = vertexes.mapToPair((PairFunction<Tuple2<Object, MapDocument>, String, MapDocument>) item -> new Tuple2<String, MapDocument>(item._2().getIdentifier(), item._2()));

        //create blocks for deduplication
        JavaPairRDD<String, List<MapDocument>> blocks = Deduper.createsortedBlocks(sc,mapDocument, dedupConf);


        //create relations by comparing only elements in the same group
        final JavaPairRDD<String,String> dedupRels = Deduper.computeRelations2(sc, blocks, dedupConf);


        final JavaRDD<Relation> isSimilarToRDD = dedupRels.map(simRel -> {
            final Relation r = new Relation();
            r.setSource(simRel._1());
            r.setTarget(simRel._2());
            r.setRelClass("isSimilarTo");
            return r;
        });

        spark.createDataset(isSimilarToRDD.rdd(), Encoders.bean(Relation.class)).write().mode("overwrite").save( DedupUtility.createSimRelPath(targetPath,entity));










    }



}
