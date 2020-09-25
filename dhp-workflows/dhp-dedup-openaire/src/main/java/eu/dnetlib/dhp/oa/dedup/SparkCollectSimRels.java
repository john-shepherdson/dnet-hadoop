package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SparkCollectSimRels extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkCollectSimRels.class);

    Dataset<Row> simGroupsDS;
    Dataset<Row> groupsDS;

    public SparkCollectSimRels(ArgumentApplicationParser parser, SparkSession spark, Dataset<Row> simGroupsDS, Dataset<Row> groupsDS) {
        super(parser, spark);
        this.simGroupsDS = simGroupsDS;
        this.groupsDS = groupsDS;
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkBlockStats.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/collectSimRels_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();

        final String dbUrl = parser.get("postgresUrl");
        final String dbUser = parser.get("postgresUser");
        final String dbPassword = parser.get("postgresPassword");

        SparkSession spark = getSparkSession(conf);

        DataFrameReader readOptions = spark.read()
                .format("jdbc")
                .option("url", dbUrl)
                .option("user", dbUser)
                .option("password", dbPassword);

        new SparkCollectSimRels(
                parser,
                spark,
                readOptions.option("dbtable", "similarity_groups").load(),
                readOptions.option("dbtable", "groups").load()
        ).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    void run(ISLookUpService isLookUpService) throws DocumentException, ISLookUpException, IOException {

        // read oozie parameters
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final int numPartitions = Optional
                .ofNullable(parser.get("numPartitions"))
                .map(Integer::valueOf)
                .orElse(NUM_PARTITIONS);
        final String dbUrl = parser.get("postgresUrl");
        final String dbUser = parser.get("postgresUser");

        log.info("numPartitions: '{}'", numPartitions);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);
        log.info("postgresUser: {}", dbUser);
        log.info("postgresUrl: {}", dbUrl);
        log.info("postgresPassword: xxx");

        JavaPairRDD<String, List<String>> similarityGroup =
                simGroupsDS
                .toJavaRDD()
                .mapToPair(r -> new Tuple2<>(r.getString(0), r.getString(1)))
                .groupByKey()
                .mapToPair(i -> new Tuple2<>(i._1(), StreamSupport.stream(i._2().spliterator(), false)
                        .collect(Collectors.toList())));

        JavaPairRDD<String, String> groupIds =
                groupsDS
                .toJavaRDD()
                .mapToPair(r -> new Tuple2<>(r.getString(0), r.getString(1)));

        JavaRDD<Tuple2<Tuple2<String, String>, List<String>>> groups = similarityGroup
                .leftOuterJoin(groupIds)
                .filter(g -> g._2()._2().isPresent())
                .map(g -> new Tuple2<>(new Tuple2<>(g._1(), g._2()._2().get()), g._2()._1()));

        JavaRDD<Relation> relations = groups.flatMap(g -> {
            String firstId = g._2().get(0);
            List<Relation> rels = new ArrayList<>();

            for (String id : g._2()) {
                if (!firstId.equals(id))
                    rels.add(createSimRel(firstId, id, g._1()._2()));
            }

            return rels.iterator();
        });

        Dataset<Relation> resultRelations = spark.createDataset(
                    relations.filter(r -> r.getRelType().equals("resultResult")).rdd(),
                    Encoders.bean(Relation.class)
        ).repartition(numPartitions);

        Dataset<Relation> organizationRelations = spark.createDataset(
                relations.filter(r -> r.getRelType().equals("organizationOrganization")).rdd(),
                Encoders.bean(Relation.class)
        ).repartition(numPartitions);

        for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {
            switch(dedupConf.getWf().getSubEntityValue()){
                case "organization":
                    savePostgresRelation(organizationRelations, workingPath, actionSetId, "organization");
                    break;
                default:
                    savePostgresRelation(resultRelations, workingPath, actionSetId, dedupConf.getWf().getSubEntityValue());
                    break;
            }
        }

    }

    private Relation createSimRel(String source, String target, String entity) {
        final Relation r = new Relation();
        r.setSubRelType("dedupSimilarity");
        r.setRelClass("isSimilarTo");
        r.setDataInfo(new DataInfo());

        switch (entity) {
            case "result":
                r.setSource("50|" + source);
                r.setTarget("50|" + target);
                r.setRelType("resultResult");
                break;
            case "organization":
                r.setSource("20|" + source);
                r.setTarget("20|" + target);
                r.setRelType("organizationOrganization");
                break;
            default:
                throw new IllegalArgumentException("unmanaged entity type: " + entity);
        }
        return r;
    }

    private void savePostgresRelation(Dataset<Relation> newRelations, String workingPath, String actionSetId, String entityType) {
        newRelations
                .write()
                .mode(SaveMode.Append)
                .parquet(DedupUtility.createSimRelPath(workingPath, actionSetId, entityType));
    }

}