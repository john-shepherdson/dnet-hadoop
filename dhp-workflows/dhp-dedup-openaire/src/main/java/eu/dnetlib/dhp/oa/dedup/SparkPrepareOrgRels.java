package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class SparkPrepareOrgRels extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkCreateDedupRecord.class);

    public static final String ROOT_TRUST = "0.8";
    public static final String PROVENANCE_ACTION_CLASS = "sysimport:dedup";
    public static final String PROVENANCE_ACTIONS = "dnet:provenanceActions";

    public SparkPrepareOrgRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCreateSimRels.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/prepareOrgRels_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        new SparkCreateDedupRecord(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final String apiUrl = parser.get("apiUrl");
        final String dbUrl = parser.get("dbUrl");
        final String dbTable = parser.get("dbTable");
        final String dbUser = parser.get("dbUser");
        final String dbPwd = parser.get("dbPwd");

        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);
        log.info("apiUrl:        '{}'", apiUrl);
        log.info("dbUrl:         '{}'", dbUrl);
        log.info("dbUser:        '{}'", dbUser);
        log.info("table:         '{}'", dbTable);
        log.info("dbPwd:         '{}'", "xxx");

        final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");
        final String entityPath = DedupUtility.createEntityPath(graphBasePath, "organization");

        Dataset<OrgSimRel> relations = createRelations(spark, mergeRelPath, entityPath);

        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUser);
        connectionProperties.put("password", dbPwd);

        relations.write().mode(SaveMode.Overwrite).jdbc(dbUrl, dbTable, connectionProperties);

        if (!apiUrl.isEmpty())
            updateSimRels(apiUrl);

    }

    public static Dataset<OrgSimRel> createRelations(
            final SparkSession spark,
            final String mergeRelsPath,
            final String entitiesPath) {

        // <id, json_entity>
        Dataset<Tuple2<String, Organization>> entities = spark
                .read()
                .textFile(entitiesPath)
                .map(
                        (MapFunction<String, Tuple2<String, Organization>>) it -> {
                            Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
                            return new Tuple2<>(entity.getId(), entity);
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

        Dataset<Tuple2<String, String>> relations = spark.createDataset(
                spark
                        .read()
                        .load(mergeRelsPath)
                        .as(Encoders.bean(Relation.class))
                        .where("relClass == 'merges'")
                        .toJavaRDD()
                        .mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget()))
                        .groupByKey()
                        .flatMap(g -> {
                            List<Tuple2<String, String>> rels = new ArrayList<>();
                            for (String id1 : g._2()) {
                                for (String id2 : g._2()) {
                                    if (!id1.equals(id2))
                                        if (id1.contains("openorgs"))
                                            rels.add(new Tuple2<>(id1, id2));
                                }
                            }
                            return rels.iterator();
                        }).rdd(),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        return relations
                .joinWith(entities, relations.col("_2").equalTo(entities.col("_1")), "inner")
                .map(
                        (MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Organization>>, OrgSimRel>)r ->
                                new OrgSimRel(
                                        r._1()._2(),
                                        r._2()._2().getOriginalId().get(0),
                                        r._2()._2().getLegalname().getValue(),
                                        r._2()._2().getLegalshortname().getValue(),
                                        r._2()._2().getCountry().getClassid(),
                                        r._2()._2().getWebsiteurl().getValue(),
                                        r._2()._2().getCollectedfrom().get(0).getValue()
                                ),
                        Encoders.bean(OrgSimRel.class)
                );

    }

    private static String updateSimRels(final String apiUrl) throws IOException {
        final HttpGet req = new HttpGet(apiUrl);
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            try (final CloseableHttpResponse response = client.execute(req)) {
                return IOUtils.toString(response.getEntity().getContent());
            }
        }
    }

}
