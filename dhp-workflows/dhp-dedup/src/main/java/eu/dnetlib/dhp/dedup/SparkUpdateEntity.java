package eu.dnetlib.dhp.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import scala.Tuple2;

import java.io.IOException;

public class SparkUpdateEntity {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkUpdateEntity.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/updateEntity_parameters.json")));
        parser.parseArgument(args);

        new SparkUpdateEntity().run(parser);
    }

    public void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String dedupGraphPath = parser.get("dedupGraphPath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");

        try (SparkSession spark = getSparkSession(parser)) {

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            for (DedupConfig dedupConf : DedupUtility.getConfigurations(isLookUpUrl, actionSetId)) {

                String subEntity = dedupConf.getWf().getSubEntityValue();

                final Dataset<Relation> df = spark.read().load(DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity)).as(Encoders.bean(Relation.class));
                final JavaPairRDD<String, String> mergedIds = df
                        .where("relClass == 'merges'")
                        .select(df.col("target"))
                        .distinct()
                        .toJavaRDD()
                        .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<>(r.getString(0), "d"));

                final JavaRDD<String> sourceEntity = sc.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity));

                final JavaRDD<String> dedupEntity = sc.textFile(DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity));

                JavaPairRDD<String, String> entitiesWithId = sourceEntity.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), s), s));

                Class<? extends Oaf> mainClass;
                switch (subEntity) {
                    case "publication":
                        mainClass = Publication.class;
                        break;
                    case "dataset":
                        mainClass = eu.dnetlib.dhp.schema.oaf.Dataset.class;
                        break;
                    case "datasource":
                        mainClass = Datasource.class;
                        break;
                    case "software":
                        mainClass = Software.class;
                        break;
                    case "organization":
                        mainClass = Organization.class;
                        break;
                    case "otherresearchproduct":
                        mainClass = OtherResearchProduct.class;
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal type " + subEntity);
                }

                JavaRDD<String> map = entitiesWithId.leftOuterJoin(mergedIds).map(k -> k._2()._2().isPresent() ? updateDeletedByInference(k._2()._1(), mainClass) : k._2()._1());
                map.union(dedupEntity).saveAsTextFile(dedupGraphPath + "/" + subEntity, GzipCodec.class);
            }
        }

    }

    private static <T extends Oaf> String updateDeletedByInference(final String json, final Class<T> clazz) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Oaf entity = mapper.readValue(json, clazz);
            if (entity.getDataInfo()== null)
                entity.setDataInfo(new DataInfo());
            entity.getDataInfo().setDeletedbyinference(true);
            return mapper.writeValueAsString(entity);
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert json", e);
        }
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkUpdateEntity.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
