package eu.dnetlib.dhp.oa.dedup.local;

import com.google.common.collect.Lists;
import eu.dnetlib.dhp.oa.dedup.DedupRecordFactory;
import eu.dnetlib.dhp.oa.dedup.DedupUtility;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDeduper;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.junit.jupiter.MockitoExtension;
import org.spark_project.guava.hash.Hashing;
import scala.Tuple2;
import com.kwartile.lib.cc.ConnectedComponent;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.dnetlib.dhp.oa.dedup.DedupRecordFactory.*;
import static eu.dnetlib.dhp.oa.dedup.SparkCreateDedupRecord.ROOT_TRUST;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkDedupLocalTest extends DedupLocalTestUtils {

    static SparkSession spark;
    static DedupConfig config;
    static JavaSparkContext context;

    final static String entitiesPath;

    static {
        try {
            entitiesPath = Paths
                    .get(SparkDedupLocalTest.class.getResource("/eu/dnetlib/dhp/dedup/entities/publication").toURI())
                    .toFile()
                    .getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    final String dedupConfPath = Paths
            .get(SparkDedupLocalTest.class.getResource("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json").toURI())
            .toFile()
            .getAbsolutePath();
    final static String subEntity = "publication";
    final static String entity = "result";

    final static String workingPath    = "/tmp/working_dir";
    final static String numPartitions  = "20";

    final static int MAX_ACCEPTANCE_DATE = 20;

    public static DataInfo dataInfo;

    private static SparkDeduper deduper;

    final static String simRelsPath     = workingPath + "/simrels";
    final static String mergeRelsPath   = workingPath + "/mergerels";
    final static String groupEntityPath = workingPath + "/groupentities";

    final static String groundTruthFieldJPath = "$.orcid";

    public SparkDedupLocalTest() throws URISyntaxException {
    }

    public static void cleanup() throws IOException {
        //remove directories to clean workspace
        FileUtils.deleteDirectory(new File(simRelsPath));
        FileUtils.deleteDirectory(new File(mergeRelsPath));
        FileUtils.deleteDirectory(new File(groupEntityPath));
    }

    @BeforeAll
    public void setup() throws IOException {

        cleanup();

        config = DedupConfig.load(readFileFromHDFS(dedupConfPath));

        spark = SparkSession
                .builder()
                .appName("Deduplication")
                .master("local[*]")
                .getOrCreate();
        context = JavaSparkContext.fromSparkContext(spark.sparkContext());

        deduper = new SparkDeduper(config);

        dataInfo = getDataInfo(config);
    }

    @AfterAll
    public static void finalCleanUp() throws IOException {
        cleanup();
    }

    protected static String readFileFromHDFS(String filePath) throws IOException {

        Path path=new Path(filePath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        try {
            return String.join("", br.lines().collect(Collectors.toList()));
        } finally {
            br.close();
        }
    }

    @Test //full deduplication workflow test
    @Disabled
    public void deduplicationTest()  {

        long before_simrels = System.currentTimeMillis();

        Dataset<?> simRels = spark
                .read()
                .textFile(entitiesPath)
                .transform(deduper.model().parseJsonDataset())
                .transform(deduper.dedup())
                .distinct()
                .map((MapFunction<Row, Relation>) t ->
                                DedupUtility.createSimRel(t.getStruct(0).getString(0), t.getStruct(0).getString(1), entity), Encoders.bean(Relation.class)
                );

        long simrels_time = System.currentTimeMillis() - before_simrels;

        long simrels_number = simRels.count();

        long before_mergerels = System.currentTimeMillis();

        UserDefinedFunction hashUDF = functions
                .udf(
                        (String s) -> hash(s), DataTypes.LongType);

        // <hash(id), id>
        Dataset<Row> vertexIdMap = simRels
                .select("source", "target")
                .selectExpr("source as id")
                .union(simRels.selectExpr("target as id"))
                .distinct()
                .withColumn("vertexId", hashUDF.apply(functions.col("id")));

        // transform simrels into pairs of numeric ids
        final Dataset<Row> edges = simRels
                .select("source", "target")
                .withColumn("source", hashUDF.apply(functions.col("source")))
                .withColumn("target", hashUDF.apply(functions.col("target")));

        // resolve connected components
        // ("vertexId", "groupId")
        Dataset<Row> cliques = ConnectedComponent
                .runOnPairs(edges, 50, spark);

        // transform "vertexId" back to its original string value
        // groupId is kept numeric as its string value is not used
        // ("id", "groupId")
        Dataset<Relation> mergeRels = cliques
                .join(vertexIdMap, JavaConversions.asScalaBuffer(Collections.singletonList("vertexId")), "inner")
                .drop("vertexId")
                .distinct()
                .flatMap((FlatMapFunction<Row, Relation>) (Row r) -> {
                    ArrayList<Relation> res = new ArrayList<>();

                    String id = r.getAs("id");
                    String groupId = r.getAs("groupId").toString();
                    res.add(rel(null, groupId, id, ModelConstants.MERGES, config));
                    res.add(rel(null, id, groupId, ModelConstants.IS_MERGED_IN, config));

                    return res.iterator();
                }, Encoders.bean(Relation.class));


        long mergerels_time = System.currentTimeMillis() - before_mergerels;

        long mergerels_number = mergeRels.count();

        long before_dedupentity = System.currentTimeMillis();

        final Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));
        final Encoder<OafEntity> beanEncoder = Encoders.bean(clazz);
        final Encoder<OafEntity> kryoEncoder = Encoders.kryo(clazz);

        Dataset<Row> entities = spark
                .read()
                .schema(Encoders.bean(clazz).schema())
                .json(entitiesPath)
                .as(beanEncoder)
                .map(
                        (MapFunction<OafEntity, Tuple2<String, OafEntity>>) entity -> {
                            return new Tuple2<>(entity.getId(), entity);
                        },
                        Encoders.tuple(Encoders.STRING(), kryoEncoder))
                .selectExpr("_1 AS id", "_2 AS kryoObject");

        // <source, target>: source is the dedup_id, target is the id of the mergedIn
        Dataset<Row> mergeRelsRow = mergeRels
                .where("relClass == 'merges'")
                .selectExpr("source as dedupId", "target as id");

        Dataset<OafEntity> dedupRecords = mergeRelsRow
                .join(entities, JavaConversions.asScalaBuffer(Collections.singletonList("id")), "left")
                .select("dedupId", "id", "kryoObject")
                .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING(), kryoEncoder))
                .groupByKey((MapFunction<Tuple3<String, String, OafEntity>, String>) Tuple3::_1, Encoders.STRING())
                .flatMapGroups(
                        (FlatMapGroupsFunction<String, Tuple3<String, String, OafEntity>, OafEntity>) (dedupId, it) -> {
                            if (!it.hasNext())
                                return Collections.emptyIterator();

                            final ArrayList<OafEntity> cliques_ = new ArrayList<>();

                            final ArrayList<String> aliases = new ArrayList<>();

                            final HashSet<String> acceptanceDate = new HashSet<>();

                            while (it.hasNext()) {
                                Tuple3<String, String, OafEntity> t = it.next();
                                OafEntity entity = t._3();

                                if (entity == null) {
                                    aliases.add(t._2());
                                } else {
                                    cliques_.add(entity);

                                    if (acceptanceDate.size() < MAX_ACCEPTANCE_DATE) { //max acceptance date
                                        if (Result.class.isAssignableFrom(entity.getClass())) {
                                            Result result = (Result) entity;
                                            if (result.getDateofacceptance() != null
                                                    && StringUtils.isNotBlank(result.getDateofacceptance().getValue())) {
                                                acceptanceDate.add(result.getDateofacceptance().getValue());
                                            }
                                        }
                                    }
                                }

                            }

                            if (acceptanceDate.size() >= MAX_ACCEPTANCE_DATE || cliques_.isEmpty()) {
                                return Collections.emptyIterator();
                            }

                            OafEntity mergedEntity = MergeUtils.mergeGroup(dedupId, cliques_.iterator());
                            // dedup records do not have date of transformation attribute
                            mergedEntity.setDateoftransformation(null);

                            return Stream
                                    .concat(
                                            Stream
                                                    .of(dedupId)
                                                    .map(id -> createDedupOafEntity(id, mergedEntity, dataInfo, before_dedupentity)),
                                            aliases
                                                    .stream()
                                                    .map(id -> createMergedDedupAliasOafEntity(id, mergedEntity, dataInfo, before_dedupentity)))
                                    .iterator();

                        }, beanEncoder);


        long dedupentity_time = System.currentTimeMillis() - before_dedupentity;

        long dedupentity_number = dedupRecords.count();

        System.out.println("Number of simrels                   : " + simrels_number);
        System.out.println("Number of mergerels                 : " + mergerels_number);
        System.out.println("Number of dedupentities             : " + dedupentity_number);
        System.out.println("Total time for simrels creation     : " + simrels_time);
        System.out.println("Total time for mergerels creation   : " + mergerels_time);
        System.out.println("Total time for dedupentity creation : " + dedupentity_time);

    }

//    @Test //test the match between two JSON
//    @Disabled
//    public void matchTest() throws Exception {
//
//        String json1 = "{\"author\":[{\"fullname\":\"Lubarsky, R.\",\"name\":\"R.\",\"pid\":[],\"rank\":1,\"surname\":\"Lubarsky\"},{\"fullname\":\"Plunkett, O. A.\",\"name\":\"O. A.\",\"pid\":[],\"rank\":2,\"surname\":\"Plunkett\"}],\"bestaccessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":[{\"key\":\"10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357\",\"value\":\"PubMed Central\"},{\"key\":\"10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c\",\"value\":\"Europe PubMed Central\"}],\"context\":[],\"contributor\":[],\"country\":[],\"coverage\":[],\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"dateofacceptance\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"1955-08-01\"},\"dateofcollection\":\"2020-08-03T18:38:58Z\",\"dateoftransformation\":\"\",\"description\":[],\"eoscifguidelines\":[],\"externalReference\":[],\"extraInfo\":[],\"format\":[],\"fulltext\":[],\"id\":\"50|pmid________::df91c1110a2cad1c2e91ca9ef3f1793b\",\"instance\":[{\"accessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"alternateIdentifier\":[],\"collectedfrom\":{\"key\":\"10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357\",\"value\":\"PubMed Central\"},\"dateofacceptance\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"1955-08-01\"},\"distributionlocation\":\"\",\"hostedby\":{\"key\":\"10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c\",\"value\":\"Europe PubMed Central\"},\"instancetype\":{\"classid\":\"0038\",\"classname\":\"Other literature type\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmc\",\"classname\":\"PubMed Central ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"PMC357660\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmid\",\"classname\":\"PubMed ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"13251984\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"https://europepmc.org/articles/PMC357660/\"]},{\"accessright\":{\"classid\":\"UNKNOWN\",\"classname\":\"not available\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"alternateIdentifier\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.1128/jb.70.2.182-186.1955\"}],\"collectedfrom\":{\"key\":\"10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c\",\"value\":\"Europe PubMed Central\"},\"dateofacceptance\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"1955-08-01\"},\"hostedby\":{\"dataInfo\":{\"deletedbyinference\":false,\"invisible\":false},\"key\":\"10|openaire____::55045bd2a65019fd8e6741a755395c8c\",\"value\":\"Unknown Repository\"},\"instancetype\":{\"classid\":\"0001\",\"classname\":\"Article\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmid\",\"classname\":\"PubMed ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"13251984\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmc\",\"classname\":\"PubMed Central ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"PMC357660\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"https://pubmed.ncbi.nlm.nih.gov/13251984\"]}],\"language\":{\"classid\":\"eng\",\"classname\":\"English\",\"schemeid\":\"dnet:languages\",\"schemename\":\"dnet:languages\"},\"lastupdatetimestamp\":1688084171694,\"measures\":[],\"oaiprovenance\":{\"originDescription\":{\"altered\":true,\"baseURL\":\"https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi\",\"datestamp\":\"2004-03-04\",\"harvestDate\":\"2018-07-12T21:51:08.484Z\",\"identifier\":\"oai:pubmedcentral.nih.gov:357660\",\"metadataNamespace\":\"http://www.openarchives.org/OAI/2.0/oai_dc/\"}},\"originalId\":[\"oai:pubmedcentral.nih.gov:357660\",\"50|od_______267::273c0e0c3c9813aba26a0f6c22ba3217\",\"od_______267::273c0e0c3c9813aba26a0f6c22ba3217\",\"13251984\",\"PMC357660\"],\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmc\",\"classname\":\"PubMed Central ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"PMC357660\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmid\",\"classname\":\"PubMed ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"13251984\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmid\",\"classname\":\"PubMed ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"13251984\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"pmc\",\"classname\":\"PubMed Central ID\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"PMC357660\"}],\"relevantdate\":[],\"resourcetype\":{\"classid\":\"UNKNOWN\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:dataCite_resource\",\"schemename\":\"dnet:dataCite_resource\"},\"resulttype\":{\"classid\":\"publication\",\"classname\":\"publication\",\"schemeid\":\"dnet:result_typologies\",\"schemename\":\"dnet:result_typologies\"},\"source\":[],\"subject\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Coccidioides\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Articles\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"In Vitro Techniques\"}],\"title\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"main title\",\"classname\":\"main title\",\"schemeid\":\"dnet:dataCite_title\",\"schemename\":\"dnet:dataCite_title\"},\"value\":\"IN VITRO PRODUCTION OF THE SPHERULE PHASE OF COCCIDIOIDES IMMITIS1\"}]}";
//        String json2 = "{\"author\":[{\"affiliation\":[{\"value\":\"University of California, Los Angeles\"}],\"fullname\":\"R. Lubarsky\",\"pid\":[{\"qualifier\":{\"classid\":\"URL\",\"classname\":\"URL\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"https://academic.microsoft.com/#/detail/2656212943\"}],\"rank\":1},{\"affiliation\":[{\"value\":\"University of California, Los Angeles\"}],\"fullname\":\"O. A. Plunkett\",\"pid\":[{\"qualifier\":{\"classid\":\"URL\",\"classname\":\"URL\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"https://academic.microsoft.com/#/detail/2722962550\"}],\"rank\":2}],\"bestaccessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":[{\"key\":\"10|openaire____::081b82f96300b6a6e3d282bad31cb6e2\",\"value\":\"Crossref\"},{\"key\":\"10|openaire____::8ac8380272269217cb09a928c8caa993\",\"value\":\"UnpayWall\"},{\"key\":\"10|openaire____::5f532a3fc4f1ea403f37070f59a7a53a\",\"value\":\"Microsoft Academic Graph\"}],\"context\":[],\"contributor\":[],\"country\":[],\"coverage\":[],\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"dateofacceptance\":{\"value\":\"1955-08-01\"},\"dateofcollection\":\"2023-05-26T02:11:42Z\",\"description\":[],\"eoscifguidelines\":[],\"externalReference\":[],\"extraInfo\":[],\"format\":[],\"fulltext\":[],\"id\":\"50|doi_________::ffffdeb480a1d91049a54768e60930f2\",\"instance\":[{\"accessright\":{\"classid\":\"CLOSED\",\"classname\":\"Closed Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":{\"key\":\"10|openaire____::081b82f96300b6a6e3d282bad31cb6e2\",\"value\":\"Crossref\"},\"dateofacceptance\":{\"value\":\"1955-08-01\"},\"hostedby\":{\"key\":\"10|issn___print::5c2525e645c3446ba30e31d9ece7c118\",\"value\":\"Journal of Bacteriology\"},\"instancetype\":{\"classid\":\"0001\",\"classname\":\"Article\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"license\":{\"value\":\"https://journals.asm.org/non-commercial-tdm-license\"},\"measures\":[{\"id\":\"influence\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"9.711022E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"1.2116836E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"influence_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"24\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0.2549689\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"impulse\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"2\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]}],\"pid\":[{\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.1128/jb.70.2.182-186.1955\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"https://doi.org/10.1128/jb.70.2.182-186.1955\"]},{\"accessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"openAccessRoute\":\"bronze\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":{\"key\":\"10|openaire____::8ac8380272269217cb09a928c8caa993\",\"value\":\"UnpayWall\"},\"hostedby\":{\"key\":\"10|issn___print::5c2525e645c3446ba30e31d9ece7c118\",\"value\":\"Journal of Bacteriology\"},\"instancetype\":{\"classid\":\"0001\",\"classname\":\"Article\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"measures\":[{\"id\":\"influence\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"9.711022E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"1.2116836E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"influence_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"24\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0.2549689\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"impulse\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"2\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]}],\"pid\":[{\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.1128/jb.70.2.182-186.1955\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"https://jb.asm.org/content/jb/70/2/182.full.pdf\"]}],\"journal\":{\"ep\":\"186\",\"issnOnline\":\"1098-5530\",\"issnPrinted\":\"0021-9193\",\"name\":\"Journal of Bacteriology\",\"sp\":\"182\",\"vol\":\"70\"},\"language\":{\"classid\":\"und\",\"classname\":\"Undetermined\",\"schemeid\":\"dnet:languages\",\"schemename\":\"dnet:languages\"},\"lastupdatetimestamp\":1685067102083,\"measures\":[{\"id\":\"influence\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"8.600855E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"1.1509996E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"influence_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"24\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"popularity_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0.15298134\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]},{\"id\":\"impulse\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"2\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C\"}]}],\"originalId\":[\"10.1128/jb.70.2.182-186.1955\",\"50|doiboost____::ffffdeb480a1d91049a54768e60930f2\",\"94595941\"],\"pid\":[{\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.1128/jb.70.2.182-186.1955\"}],\"publisher\":{\"value\":\"American Society for Microbiology\"},\"relevantdate\":[{\"qualifier\":{\"classid\":\"created\",\"classname\":\"created\",\"schemeid\":\"dnet:dataCite_date\",\"schemename\":\"dnet:dataCite_date\"},\"value\":\"2020-01-03\"},{\"qualifier\":{\"classid\":\"published-print\",\"classname\":\"published-print\",\"schemeid\":\"dnet:dataCite_date\",\"schemename\":\"dnet:dataCite_date\"},\"value\":\"1955-08-01\"}],\"resourcetype\":{\"classid\":\"0001\",\"classname\":\"Article\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"resulttype\":{\"classid\":\"publication\",\"classname\":\"publication\",\"schemeid\":\"dnet:result_typologies\",\"schemename\":\"dnet:result_typologies\"},\"source\":[{\"value\":\"Crossref\"},{}],\"subject\":[{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Tissue culture\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Blood serum\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.61931455\"},\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"biology\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Coccidioides immitis\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Immunology\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Coccidioides\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Embryo\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Chick embryos\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.61931455\"},\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"biology.organism_classification\"},{\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Molecular Biology\"},{\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Microbiology\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"In vitro\"},{\"qualifier\":{\"classid\":\"MAG\",\"classname\":\"Microsoft Academic Graph classification\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"Microbiology\"}],\"title\":[{\"qualifier\":{\"classid\":\"alternative title\",\"classname\":\"alternative title\",\"schemeid\":\"dnet:dataCite_title\",\"schemename\":\"dnet:dataCite_title\"},\"value\":\"In vitro production of the spherule phase of Coccidioides immitis.\"},{\"qualifier\":{\"classid\":\"main title\",\"classname\":\"main title\",\"schemeid\":\"dnet:dataCite_title\",\"schemename\":\"dnet:dataCite_title\"},\"value\":\"IN VITRO PRODUCTION OF THE SPHERULE PHASE OF COCCIDIOIDES IMMITIS\"}]}";
//
//        DedupConfig config = DedupConfig.load(readFileFromHDFS(Paths
//                .get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/config/pub.prod.tree.conf.json").toURI())
//                .toFile()
//                .getAbsolutePath()));
//
//        MapDocument a = MapDocumentUtil.asMapDocumentWithJPath(config, json1);
//        MapDocument b = MapDocumentUtil.asMapDocumentWithJPath(config, json2);
//
//        boolean result = new TreeProcessor(config).compare(a,b);
//
//        System.out.println("Tree Processor Result = " + result);
//
//    }
//
//    @Test //test the keys between two JSON
//    @Disabled
//    public void blockTest() throws Exception {
//
//        String json1 = "{\"author\":[{\"affiliation\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"CSC - IT Centre for Science\"}],\"fullname\":\"Minna Lappalainen\",\"name\":\"\",\"pid\":[],\"rank\":1,\"surname\":\"\"}],\"bestaccessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":[{\"key\":\"10|opendoar____::358aee4cc897452c00244351e4d91f69\",\"value\":\"ZENODO\"}],\"context\":[{\"dataInfo\":[{\"deletedbyinference\":false,\"inferenceprovenance\":\"bulktagging\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"community:datasource\",\"classname\":\"Inferred by OpenAIRE\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.8\"}],\"id\":\"eosc\"}],\"contributor\":[],\"country\":[],\"coverage\":[],\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"dateofacceptance\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"2023-02-21\"},\"dateofcollection\":\"2023-09-05T02:17:42+0000\",\"dateoftransformation\":\"2023-09-05T10:28:52.475Z\",\"description\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"<p>This presentation gives an introduction to LUMI&#39;s new model for business.</p> <p>It was delivered at the Special Interest Group on Marketing and Communications on February 21, 2023.</p>\"}],\"eoscifguidelines\":[],\"externalReference\":[],\"extraInfo\":[],\"format\":[],\"fulltext\":[],\"id\":\"50|doi_________::7abc0a7f472e07fbb918c80213e84513\",\"instance\":[{\"accessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"alternateIdentifier\":[],\"collectedfrom\":{\"key\":\"10|opendoar____::358aee4cc897452c00244351e4d91f69\",\"value\":\"ZENODO\"},\"dateofacceptance\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"2023-02-21\"},\"hostedby\":{\"key\":\"10|opendoar____::358aee4cc897452c00244351e4d91f69\",\"value\":\"ZENODO\"},\"instancetype\":{\"classid\":\"0038\",\"classname\":\"Other literature type\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"license\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"http://creativecommons.org/licenses/by/4.0/legalcode\"},\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.5281/zenodo.7674680\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"http://dx.doi.org/10.5281/zenodo.7674680\"]}],\"language\":{\"classid\":\"eng\",\"classname\":\"English\",\"schemeid\":\"dnet:languages\",\"schemename\":\"dnet:languages\"},\"lastupdatetimestamp\":1694244806053,\"measures\":[{\"id\":\"influence\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"3.349422E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"popularity\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"4.2539727E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"influence_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"popularity_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0.0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"impulse\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]}],\"originalId\":[\"oai:zenodo.org:7674680\",\"50|od______2659::8db48a30e74218f13b023f315a108d14\"],\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.5281/zenodo.7674680\"}],\"publisher\":{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"Zenodo\"},\"relevantdate\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"issued\",\"classname\":\"issued\",\"schemeid\":\"dnet:dataCite_date\",\"schemename\":\"dnet:dataCite_date\"},\"value\":\"2023-02-21\"}],\"resourcetype\":{\"classid\":\"Presentation\",\"classname\":\"Presentation\",\"schemeid\":\"dnet:dataCite_resource\",\"schemename\":\"dnet:dataCite_resource\"},\"resulttype\":{\"classid\":\"publication\",\"classname\":\"publication\",\"schemeid\":\"dnet:result_typologies\",\"schemename\":\"dnet:result_typologies\"},\"source\":[],\"subject\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"LUMI supercomputer\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"HPC\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"High Performance Computing\"}],\"title\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"main title\",\"classname\":\"main title\",\"schemeid\":\"dnet:dataCite_title\",\"schemename\":\"dnet:dataCite_title\"},\"value\":\"LUMI for Business\"}]}";
//        String json2 = "{\"author\":[{\"affiliation\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"value\":\"CSC - IT Centre for Science\"}],\"fullname\":\"Lappalainen, Minna\",\"name\":\"Minna\",\"pid\":[],\"rank\":1,\"surname\":\"Lappalainen\"}],\"bestaccessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":[{\"key\":\"10|openaire____::9e3be59865b2c1c335d32dae2fe7b254\",\"value\":\"Datacite\"}],\"context\":[{\"dataInfo\":[{\"deletedbyinference\":false,\"inferenceprovenance\":\"bulktagging\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"community:datasource\",\"classname\":\"Inferred by OpenAIRE\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.8\"}],\"id\":\"eosc\"}],\"contributor\":[],\"country\":[],\"coverage\":[],\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"dateofacceptance\":{\"value\":\"2023-02-21\"},\"dateofcollection\":\"2023-02-24T14:37:47+0000\",\"dateoftransformation\":\"2023-02-24T14:37:47+0000\",\"description\":[{\"value\":\"This presentation gives an introduction to LUMI's new model for business. It was delivered at the Special Interest Group on Marketing and Communications on February 21, 2023.\"}],\"eoscifguidelines\":[],\"externalReference\":[],\"extraInfo\":[],\"format\":[],\"fulltext\":[],\"id\":\"50|doi_________::d92b5878ccaadd47bfdc41a36afa6914\",\"instance\":[{\"accessright\":{\"classid\":\"OPEN\",\"classname\":\"Open Access\",\"schemeid\":\"dnet:access_modes\",\"schemename\":\"dnet:access_modes\"},\"collectedfrom\":{\"key\":\"10|openaire____::9e3be59865b2c1c335d32dae2fe7b254\",\"value\":\"Datacite\"},\"dateofacceptance\":{\"value\":\"2023-02-21\"},\"hostedby\":{\"key\":\"10|opendoar____::358aee4cc897452c00244351e4d91f69\",\"value\":\"ZENODO\"},\"instancetype\":{\"classid\":\"0050\",\"classname\":\"Presentation\",\"schemeid\":\"dnet:publication_resource\",\"schemename\":\"dnet:publication_resource\"},\"license\":{\"value\":\"https://creativecommons.org/licenses/by/4.0/legalcode\"},\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.5281/zenodo.7674679\"}],\"refereed\":{\"classid\":\"0000\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:review_levels\",\"schemename\":\"dnet:review_levels\"},\"url\":[\"https://dx.doi.org/10.5281/zenodo.7674679\"]}],\"language\":{\"classid\":\"eng\",\"classname\":\"English\",\"schemeid\":\"dnet:languages\",\"schemename\":\"dnet:languages\"},\"lastupdatetimestamp\":0,\"measures\":[{\"id\":\"influence\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"3.349422E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"popularity\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"4.2539727E-9\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"influence_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"popularity_alt\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0.0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]},{\"id\":\"impulse\",\"unit\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"score\",\"value\":\"0\"},{\"dataInfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"update\",\"inferred\":true,\"invisible\":false,\"provenanceaction\":{\"classid\":\"measure:bip\",\"classname\":\"measure:bip\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"\"},\"key\":\"class\",\"value\":\"C5\"}]}],\"originalId\":[\"50|datacite____::d92b5878ccaadd47bfdc41a36afa6914\",\"10.5281/zenodo.7674679\"],\"pid\":[{\"dataInfo\":{\"deletedbyinference\":false,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"doi\",\"classname\":\"Digital Object Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"10.5281/zenodo.7674679\"}],\"publisher\":{\"value\":\"Zenodo\"},\"relevantdate\":[{\"qualifier\":{\"classid\":\"issued\",\"classname\":\"issued\",\"schemeid\":\"dnet:dataCite_date\",\"schemename\":\"dnet:dataCite_date\"},\"value\":\"2023-02-21\"}],\"resourcetype\":{\"classid\":\"UNKNOWN\",\"classname\":\"UNKNOWN\",\"schemeid\":\"dnet:dataCite_resource\",\"schemename\":\"dnet:dataCite_resource\"},\"resulttype\":{\"classid\":\"publication\",\"classname\":\"publication\",\"schemeid\":\"dnet:result_typologies\",\"schemename\":\"dnet:result_typologies\"},\"source\":[],\"subject\":[{\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"LUMI supercomputer\"},{\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"HPC\"},{\"qualifier\":{\"classid\":\"keyword\",\"classname\":\"keyword\",\"schemeid\":\"dnet:subject_classification_typologies\",\"schemename\":\"dnet:subject_classification_typologies\"},\"value\":\"High Performance Computing\"}],\"title\":[{\"qualifier\":{\"classid\":\"main title\",\"classname\":\"main title\",\"schemeid\":\"dnet:dataCite_title\",\"schemename\":\"dnet:dataCite_title\"},\"value\":\"LUMI for Business\"}]}";
//
//        DedupConfig config = DedupConfig.load(readFileFromHDFS(Paths
//                .get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/config/pub.prod.tree.conf.json").toURI())
//                .toFile()
//                .getAbsolutePath()));
//
//        MapDocument a = MapDocumentUtil.asMapDocumentWithJPath(config, json1);
//        MapDocument b = MapDocumentUtil.asMapDocumentWithJPath(config, json2);
//
//        List<MapDocument> mapDocuments = Lists.newArrayList(a, b);
//        JavaPairRDD<String, Block> sortedBlocks = Deduper.createSortedBlocks(context.parallelize(mapDocuments).mapToPair(d -> new Tuple2<>(d.getIdentifier(), d)), config);
//
//        for (Tuple2<String, Block> block: sortedBlocks.collect()) {
//            System.out.println("block key = " + block._1());
//            System.out.println("block size = " + block._2().elements());
//        }
//
//
//    }
//
//    @Test //test the dedup of a group of JSON
//    @Disabled
//    public void dedupTest() throws Exception {
//        final String entitiesPath = Paths
//                .get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/examples/software.to.fix.json").toURI())
//                .toFile()
//                .getAbsolutePath();
//
//        DedupConfig dedupConf = DedupConfig.load(readFileFromHDFS(Paths
//                .get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/config/sw.tree.conf.json").toURI())
//                .toFile()
//                .getAbsolutePath()));
//
//        JavaPairRDD<String, MapDocument> mapDocuments = context
//                .textFile(entitiesPath)
//                .mapToPair(
//                        (PairFunction<String, String, MapDocument>) s -> {
//                            MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
//                            return new Tuple2<>(d.getIdentifier(), d);
//                        })
//                .reduceByKey((a,b) -> a);
//
//        // create blocks for deduplication
//        JavaPairRDD<String, Block> blocks = Deduper.createSortedBlocks(mapDocuments, dedupConf);
//        for (Tuple2<String, Block> b : blocks.collect()) {
//            System.out.println("*******GROUPS********");
//            System.out.println("key = " + b._1());
//            System.out.println("elements = " + b._2().elements());
//            System.out.println("items = " + b._2().getDocuments().stream().map(d -> d.getIdentifier()).collect(Collectors.joining(",")));
//            System.out.println("*********************");
//        }
//
//        // create relations by comparing only elements in the same group
//        JavaRDD<Relation> relations = Deduper.computeRelations(context, blocks, dedupConf, true, false);
//        for (Relation r: relations.collect()) {
//            System.out.println("*******RELATIONS*******");
//            System.out.println("source = " + r.getSource());
//            System.out.println("target = " + r.getTarget());
//            System.out.println("***********************");
//        }
//
//        //vertexes
//        List<String> vertexes = mapDocuments.map(doc -> doc._1()).collect();
//
//        //edges
//        List<Tuple2<String, String>> edges = new ArrayList<>();
//        relations.collect().stream().forEach(r -> edges.add(new Tuple2(r.getSource(), r.getTarget())));
//
//        showGraph(vertexes, edges, mapDocuments);
//
//        cleanup();
//    }
//
//    public void showGraph(List<String> vertexes, List<Tuple2<String, String>> edges, JavaPairRDD<String, MapDocument> mapDocuments)  {
//
//        try {
//            prepareGraphParams(
//                    vertexes,
//                    edges,
//                    "/tmp/graph.html", Paths.get(DedupLocalTest.class.getResource("/graph_visualization_tool/graph_template.html").toURI()).toFile().getAbsolutePath(),
//                    mapDocuments.collectAsMap());
//            Desktop.getDesktop().browse(new File("/tmp/graph.html").toURI());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public int nodeDegree(String id, List<Tuple2<String, String>> edges) {
//        return (int) edges.stream().map(e -> e._1()).filter(s -> s.equalsIgnoreCase(id)).count();
//    }
//
//    public int minDegree(List<String> vertexes, List<Tuple2<String, String>> edges) {
//
//        int minDegree = 100;
//        for (String vertex: vertexes) {
//            int deg = nodeDegree(vertex, edges);
//            if (deg < minDegree)
//                minDegree = deg;
//        }
//        return minDegree;
//    }
//
//    @Test
//    @Disabled
//    public void asMapDocument() throws Exception {
//
//        final String json = "{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": true, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"resourcetype\": {\"classid\": \"0001\", \"classname\": \"0001\", \"schemeid\": \"dnet:dataCite_resource\", \"schemename\": \"dnet:dataCite_resource\"}, \"pid\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"handle\", \"classname\": \"Handle\", \"schemeid\": \"dnet:pid_types\", \"schemename\": \"dnet:pid_types\"}, \"value\": \"11370/8fb3e34b-47c4-4e87-a675-c889db060e19\"}], \"contributor\": [], \"bestaccessright\": {\"classid\": \"CLOSED\", \"classname\": \"Closed Access\", \"schemeid\": \"dnet:access_modes\", \"schemename\": \"dnet:access_modes\"}, \"relevantdate\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"issued\", \"classname\": \"issued\", \"schemeid\": \"dnet:dataCite_date\", \"schemename\": \"dnet:dataCite_date\"}, \"value\": \"2018-01-01\"}], \"collectedfrom\": [{\"dataInfo\": null, \"key\": \"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f\", \"value\": \"DANS (Data Archiving and Networked Services)\"}], \"id\": \"50|DansKnawCris::4ad8a5701b7d06b851966e1b323a2a95\", \"subject\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"OF-THE-ART\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"SCHOOL LEADERSHIP\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"DYNAMIC-MODEL\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"IMPLEMENTATION\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"OUTCOMES\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"keyword\", \"classname\": \"keyword\", \"schemeid\": \"dnet:subject_classification_typologies\", \"schemename\": \"dnet:subject_classification_typologies\"}, \"value\": \"To be checked by Faculty\"}], \"embargoenddate\": null, \"lastupdatetimestamp\": 1645007805304, \"author\": [{\"surname\": \"Kyriakides\", \"name\": \"Leonidas\", \"pid\": [], \"rank\": 1, \"affiliation\": [], \"fullname\": \"Kyriakides, Leonidas\"}, {\"surname\": \"Georgiou\", \"name\": \"Maria P.\", \"pid\": [], \"rank\": 2, \"affiliation\": [], \"fullname\": \"Georgiou, Maria P.\"}, {\"surname\": \"Creemers\", \"name\": \"Bert P. M.\", \"pid\": [], \"rank\": 3, \"affiliation\": [], \"fullname\": \"Creemers, Bert P. M.\"}, {\"surname\": \"Panayiotou\", \"name\": \"Anastasia\", \"pid\": [], \"rank\": 4, \"affiliation\": [], \"fullname\": \"Panayiotou, Anastasia\"}, {\"surname\": \"Reynolds\", \"name\": \"David\", \"pid\": [], \"rank\": 5, \"affiliation\": [], \"fullname\": \"Reynolds, David\"}], \"instance\": [{\"refereed\": {\"classid\": \"0000\", \"classname\": \"UNKNOWN\", \"schemeid\": \"dnet:review_levels\", \"schemename\": \"dnet:review_levels\"}, \"hostedby\": {\"dataInfo\": null, \"key\": \"10|issn___print::2763d4e6e2e870a7ffd4bf8863c6bbff\", \"value\": \"School Effectiveness and School Improvement\"}, \"accessright\": {\"classid\": \"CLOSED\", \"classname\": \"Closed Access\", \"schemeid\": \"dnet:access_modes\", \"schemename\": \"dnet:access_modes\", \"openAccessRoute\": null}, \"license\": null, \"url\": [\"\", \"http://dx.doi.org/10.1080/09243453.2017.1398761\"], \"measures\": null, \"pid\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"handle\", \"classname\": \"Handle\", \"schemeid\": \"dnet:pid_types\", \"schemename\": \"dnet:pid_types\"}, \"value\": \"11370/8fb3e34b-47c4-4e87-a675-c889db060e19\"}], \"distributionlocation\": null, \"processingchargecurrency\": null, \"alternateIdentifier\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"urn\", \"classname\": \"urn\", \"schemeid\": \"dnet:pid_types\", \"schemename\": \"dnet:pid_types\"}, \"value\": \"urn:nbn:nl:ui:11-8fb3e34b-47c4-4e87-a675-c889db060e19\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"doi\", \"classname\": \"Digital Object Identifier\", \"schemeid\": \"dnet:pid_types\", \"schemename\": \"dnet:pid_types\"}, \"value\": \"10.1080/09243453.2017.1398761\"}], \"dateofacceptance\": {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"value\": \"2018-01-01\"}, \"collectedfrom\": {\"dataInfo\": null, \"key\": \"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f\", \"value\": \"DANS (Data Archiving and Networked Services)\"}, \"processingchargeamount\": null, \"instancetype\": {\"classid\": \"0001\", \"classname\": \"Article\", \"schemeid\": \"dnet:publication_resource\", \"schemename\": \"dnet:publication_resource\"}}], \"resulttype\": {\"classid\": \"publication\", \"classname\": \"publication\", \"schemeid\": \"dnet:result_typologies\", \"schemename\": \"dnet:result_typologies\"}, \"dateofcollection\": \"2020-11-12T23:04:09.482Z\", \"fulltext\": [], \"dateoftransformation\": \"2020-11-13T01:33:00.881Z\", \"description\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"value\": \"This paper investigates the impact of national policies for improving teaching and the school learning environment (SLE) on student achievement. In each participating country (i.e., Belgium/Flanders, Cyprus, Germany, Greece, Ireland, and Slovenia), a sample of at least 50 schools was drawn and tests in mathematics and science were administered to all Grade 4 students (N = 10,742) at the beginning and end of school year 2010\\u20132011. National policies were measured through (a) content analysis of policy documents, (b) interviews with policymakers, and (c) head-teacher questionnaires. Multilevel analyses revealed that most aspects of national policies for teaching and SLE were associated with student achievement in each subject irrespective of the source of data used to measure them. Implications are, finally, drawn.\"}], \"format\": [], \"processingchargecurrency\": null, \"journal\": {\"issnPrinted\": \"0924-3453\", \"conferencedate\": null, \"vol\": \"29\", \"conferenceplace\": null, \"name\": \"School Effectiveness and School Improvement\", \"iss\": \"2\", \"sp\": \"171\", \"edition\": \"\", \"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"issnOnline\": \"\", \"ep\": \"203\", \"issnLinking\": \"\"}, \"measures\": null, \"dateofacceptance\": {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"value\": \"2018-01-01\"}, \"coverage\": [], \"processingchargeamount\": null, \"externalReference\": [], \"publisher\": null, \"language\": {\"classid\": \"eng\", \"classname\": \"English\", \"schemeid\": \"dnet:languages\", \"schemename\": \"dnet:languages\"}, \"oaiprovenance\": {\"originDescription\": {\"metadataNamespace\": \"\", \"harvestDate\": \"2020-11-12T23:04:09.482Z\", \"baseURL\": \"http%3A%2F%2Fservices.nod.dans.knaw.nl%2Foa-cerif\", \"datestamp\": \"2020-11-12T00:51:43Z\", \"altered\": true, \"identifier\": \"oai:services.nod.dans.knaw.nl:Publications/rug:oai:pure.rug.nl:publications/8fb3e34b-47c4-4e87-a675-c889db060e19\"}}, \"country\": [], \"extraInfo\": [], \"originalId\": [\"oai:services.nod.dans.knaw.nl:Publications/rug:oai:pure.rug.nl:publications/8fb3e34b-47c4-4e87-a675-c889db060e19\", \"50|DansKnawCris::4ad8a5701b7d06b851966e1b323a2a95\"], \"source\": [], \"context\": [], \"title\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"The impact of national educational policies on student achievement\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"sysimport:crosswalk:datasetarchive\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": false, \"inferenceprovenance\": \"\", \"invisible\": false, \"trust\": \"0.9\"}, \"qualifier\": {\"classid\": \"subtitle\", \"classname\": \"subtitle\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"a European study\"}]}";
//
//        DedupConfig dedupConfig = DedupConfig.load(readFileFromHDFS(
//                Paths.get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/config/pub.new.tree.conf.json").toURI()).toFile().getAbsolutePath()
//        ));
//
//        final MapDocument mapDocument = MapDocumentUtil.asMapDocumentWithJPath(dedupConfig, json);
//
//        for(String field: mapDocument.getFieldMap().keySet()) {
//            System.out.println(field + ": " + mapDocument.getFieldMap().get(field).stringValue());
//        }
//    }
//
//    @Test
//    @Disabled
//    public void noMatchTest() throws Exception {
//
//        //custom parameters for this test
//        DedupConfig dedupConfig = DedupConfig.load(readFileFromHDFS(
//                Paths.get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/config/pub.new.tree.conf.json").toURI()).toFile().getAbsolutePath()
//        ));
//        String inputPath = Paths.get(DedupLocalTest.class.getResource("/eu/dnetlib/pace/examples/publications.dump.1000.json").toURI()).toFile().getAbsolutePath();
//        String simRelsPath = workingPath + "/simrels";
//
//        Deduper.createSimRels(
//                dedupConfig,
//                spark,
//                inputPath,
//                simRelsPath,
//                true,
//                true
//        );
//        Dataset<Relation> noMatches = spark.read().load(simRelsPath).as(Encoders.bean(Relation.class));
//
//        System.out.println("noMatches = " + noMatches.count());
//
//        noMatches.foreach((ForeachFunction<Relation>) r -> System.out.println(r.getSource() + " " + r.getTarget()));
//
//        FileUtils.deleteDirectory(new File(workingPath));
//    }

    public static long hash(final String id) {
        return Hashing.murmur3_128().hashString(id).asLong();
    }

    private static Relation rel(String pivot, String source, String target, String relClass, DedupConfig dedupConf) {

        String entityType = dedupConf.getWf().getEntityType();

        Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass(relClass);
        r.setRelType(entityType + entityType.substring(0, 1).toUpperCase() + entityType.substring(1));
        r.setSubRelType(ModelConstants.DEDUP);

        DataInfo info = new DataInfo();
        info.setDeletedbyinference(false);
        info.setInferred(true);
        info.setInvisible(false);
        info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
        Qualifier provenanceAction = new Qualifier();
        provenanceAction.setClassid(PROVENANCE_DEDUP);
        provenanceAction.setClassname(PROVENANCE_DEDUP);
        provenanceAction.setSchemeid(DNET_PROVENANCE_ACTIONS);
        provenanceAction.setSchemename(DNET_PROVENANCE_ACTIONS);
        info.setProvenanceaction(provenanceAction);

        // TODO calculate the trust value based on the similarity score of the elements in the CC

        r.setDataInfo(info);

        if (pivot != null) {
            KeyValue pivotKV = new KeyValue();
            pivotKV.setKey("pivot");
            pivotKV.setValue(pivot);

            r.setProperties(Arrays.asList(pivotKV));
        }
        return r;
    }

    private static OafEntity createDedupOafEntity(String id, OafEntity base, DataInfo dataInfo, long ts) {
        try {
            OafEntity res = (OafEntity) BeanUtils.cloneBean(base);
            res.setId(id);
            res.setDataInfo(dataInfo);
            res.setLastupdatetimestamp(ts);
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static OafEntity createMergedDedupAliasOafEntity(String id, OafEntity base, DataInfo dataInfo, long ts) {
        try {
            OafEntity res = createDedupOafEntity(id, base, dataInfo, ts);
            DataInfo ds = (DataInfo) BeanUtils.cloneBean(dataInfo);
            ds.setDeletedbyinference(true);
            res.setDataInfo(ds);
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static DataInfo getDataInfo(DedupConfig dedupConf) {
        DataInfo info = new DataInfo();
        info.setDeletedbyinference(false);
        info.setInferred(true);
        info.setInvisible(false);
        info.setTrust(ROOT_TRUST);
        info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
        Qualifier provenance = new Qualifier();
        provenance.setClassid(PROVENANCE_DEDUP);
        provenance.setClassname(PROVENANCE_DEDUP);
        provenance.setSchemeid(DNET_PROVENANCE_ACTIONS);
        provenance.setSchemename(DNET_PROVENANCE_ACTIONS);
        info.setProvenanceaction(provenance);
        return info;
        }

}