package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.maxclique.AllMaxCliqueFinder;
import eu.dnetlib.dhp.oa.dedup.maxclique.MaxCliqueFinderFacade;
import eu.dnetlib.dhp.oa.dedup.maxclique.support.SimRelWeigher;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDeduper;
import eu.dnetlib.pace.model.SparkModel;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import scala.Tuple2;
import scala.Tuple3;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkSplitGroups extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkSplitGroups.class);
    private static final StructType rowSchema = new StructType(new StructField[]{
        new StructField("source", DataTypes.StringType, false, Metadata.empty()),
                new StructField("target", DataTypes.StringType, false, Metadata.empty())
    });

    public SparkSplitGroups(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCreateMergeRels.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/splitGroups_parameters.json")));
        parser.parseArgument(args);

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookupUrl {}", isLookUpUrl);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        new SparkSplitGroups(parser, getSparkWithHiveSession(conf))
                .run(ISLookupClientFactory.getLookUpService(isLookUpUrl));
    }

    @Override
    void run(ISLookUpService isLookUpService) throws DocumentException, IOException, ISLookUpException, SAXException {

        // read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final int numPartitions = Optional
                .ofNullable(parser.get("numPartitions"))
                .map(Integer::valueOf)
                .orElse(NUM_PARTITIONS);

        log.info("numPartitions: '{}'", numPartitions);
        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        // for each dedup configuration
        for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

            final String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Processing mergerels for: '{}'", subEntity);

            final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

            SparkDeduper deduper = new SparkDeduper(dedupConf);

            // compute negative constraints and append to the entities
            Dataset<Row> entities = spark
                    .read()
                    .textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
                    .transform(deduper.model().parseJsonDataset());
            entities = appendNegativeConstraints(spark, entities, graphBasePath, subEntity);

            Dataset<Row> rawMergeRels = spark
                    .read()
                    .load(mergeRelPath)
                    .as(Encoders.bean(Relation.class))
                    .where("relClass == 'merges'")
                    .select("source", "target")
                    .join(entities, col("target").equalTo(entities.col("identifier")))
                    .withColumnRenamed("source", "groupId");

            // compute the list of conflictual ids (groups containing conflicts)
            Dataset<Row> conflictualIds = rawMergeRels
                    .select("groupId", "negativeConstraints")
                    .groupBy("groupId")
                    .agg(flatten(collect_list(col("negativeConstraints"))).alias("allConstraints"))
                    .where(size(col("allConstraints")).notEqual(size(array_distinct(col("allConstraints")))))
                    .select("groupId");

            Dataset<Row> cleanMergeRels = rawMergeRels.join(conflictualIds, rawMergeRels.col("groupId").equalTo(conflictualIds.col("groupId")), "left_anti")
                    .select(col("groupId").as("source"), col("target"));
            Dataset<Row> conflictualMergeRels = rawMergeRels.join(conflictualIds, rawMergeRels.col("groupId").equalTo(conflictualIds.col("groupId")), "left_semi");

            Dataset<Row> splitMergeRels = conflictualMergeRels
                    .groupByKey((MapFunction<Row, String>) t -> t.getAs("groupId"), Encoders.STRING())
                    .flatMapGroups((FlatMapGroupsFunction<String, Row, Row>) (key, values) -> {
                                String prefix = key.split("::")[0];
                                String md5 = key.split("::")[1];

                                List<Row> mergeRels = new ArrayList<>();
                                AllMaxCliqueFinder<Row> finder = new AllMaxCliqueFinder<>(
                                        () -> values,
                                        MaxCliqueFinderFacade.getInstance(),
                                        new SimRelWeigher(dedupConf)
                                );

                                AtomicInteger index = new AtomicInteger(0);
                                finder.iterator().forEachRemaining(row -> {
                                    if (!row.isEmpty() || !row.isSingleMember()) {
                                        for (Row r : row.members()) {
                                            mergeRels.add(RowFactory.create(prefix.substring(0, prefix.length() - 1) + index + "::" + md5, r.getAs("identifier")));
                                        }
                                        index.addAndGet(1);
                                    }
                                });

                                return mergeRels.iterator();
                            },
                            RowEncoder.apply(rowSchema)
                    );

            Dataset<Relation> output = cleanMergeRels
                    .union(splitMergeRels)
                    .flatMap(
                        (FlatMapFunction<Row, Relation>) r -> {
                            String dedupId = r.getString(0);
                            String id = r.getString(1);

                            ArrayList<Relation> res = new ArrayList<>();
                            res.add(rel(dedupId, id, ModelConstants.MERGES, dedupConf));
                            res.add(rel(id, dedupId, ModelConstants.IS_MERGED_IN, dedupConf));

                            return res.iterator();
                            }, Encoders.bean(Relation.class)
                    );

            saveParquet(output, mergeRelPath + "_tmp", SaveMode.Overwrite);
            renameParquet(spark, mergeRelPath + "_tmp", mergeRelPath);
        }
    }

    // <raw_id, labels>: when the label is the same, two entities cannot be together
    public static Dataset<Row> appendNegativeConstraints(SparkSession spark, Dataset<Row> entities, String graphBasePath, String subEntity) {

        switch (subEntity) {
            case "organization":
                Dataset<Row> families = OpenorgsUtility.createFamilies(spark, graphBasePath + "/relation", ModelConstants.IS_PARENT_OF);

                // add nwo label to negative constraints
                entities = entities.withColumn(
                                "negativeConstraints",
                                when(col("identifier").contains("nwo"), array(lit("nwo"))).otherwise(array()));
                return entities
                        .join(families, entities.col("identifier").equalTo(families.col("id")), "left")
                        .withColumn("negativeConstraints",
                                when(families.col("groupId").isNotNull(),
                                        array_union(col("negativeConstraints"), array(families.col("groupId").cast("string"))))
                                        .otherwise(col("negativeConstraints")))
                        .drop("id", "groupId");

            default:
                return spark.emptyDataFrame();
        }

    }

    private static Relation rel(String source, String target, String relClass, DedupConfig dedupConf) {

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

        r.setDataInfo(info);
        return r;
    }

}
