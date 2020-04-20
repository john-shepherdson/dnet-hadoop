package eu.dnetlib.dhp.migration.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class TransformActions implements Serializable {

    private static final Log log = LogFactory.getLog(TransformActions.class);
    private static final String SEPARATOR = "/";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                MigrateActionSet.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/migration/transform_actionsets_parameters.json")));
        parser.parseArgument(args);

        new TransformActions().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, IOException {

        final String isLookupUrl = parser.get("isLookupUrl");
        log.info("isLookupUrl: " + isLookupUrl);

        final String inputPaths = parser.get("inputPaths");

        if (StringUtils.isBlank(inputPaths)) {
            throw new RuntimeException("empty inputPaths");
        }
        log.info("inputPaths: " + inputPaths);

        final String targetBaseDir = getTargetBaseDir(isLookupUrl);

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
            final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

            for (String sourcePath : Lists.newArrayList(Splitter.on(",").split(inputPaths))) {

                LinkedList<String> pathQ =
                        Lists.newLinkedList(Splitter.on(SEPARATOR).split(sourcePath));

                final String rawset = pathQ.pollLast();
                final String actionSetDirectory = pathQ.pollLast();

                final Path targetDirectory =
                        new Path(
                                targetBaseDir
                                        + SEPARATOR
                                        + actionSetDirectory
                                        + SEPARATOR
                                        + rawset);

                if (fs.exists(targetDirectory)) {
                    log.info(String.format("found target directory '%s", targetDirectory));
                    fs.delete(targetDirectory, true);
                    log.info(String.format("deleted target directory '%s", targetDirectory));
                }

                log.info(
                        String.format(
                                "transforming actions from '%s' to '%s'",
                                sourcePath, targetDirectory));

                sc.sequenceFile(sourcePath, Text.class, Text.class)
                        .map(
                                a ->
                                        eu.dnetlib.actionmanager.actions.AtomicAction.fromJSON(
                                                a._2().toString()))
                        .map(a -> doTransform(a))
                        .filter(Objects::isNull)
                        .filter(a -> a.getPayload() == null)
                        .map(a -> new ObjectMapper().writeValueAsString(a))
                        .saveAsTextFile(targetDirectory.toString(), GzipCodec.class);
            }
        }
    }

    private Text transformAction(eu.dnetlib.actionmanager.actions.AtomicAction aa)
            throws InvalidProtocolBufferException, JsonProcessingException {
        final Text out = new Text();
        final ObjectMapper mapper = new ObjectMapper();
        if (aa.getTargetValue() != null && aa.getTargetValue().length > 0) {
            out.set(mapper.writeValueAsString(doTransform(aa)));
        }
        return out;
    }

    private AtomicAction<Relation> getRelationAtomicAction(String atomicaActionId) {
        final String[] splitId = atomicaActionId.split("@");

        String source = splitId[0];
        String target = splitId[2];

        String[] relSemantic = splitId[1].split("_");

        Relation rel = new Relation();
        rel.setSource(source);
        rel.setTarget(target);
        rel.setRelType(relSemantic[0]);
        rel.setSubRelType(relSemantic[1]);
        rel.setRelClass(relSemantic[2]);

        DataInfo d = new DataInfo();
        d.setDeletedbyinference(false);
        d.setInferenceprovenance("deduplication");
        d.setInferred(true);
        d.setInvisible(false);
        Qualifier provenanceaction = new Qualifier();

        provenanceaction.setClassid("deduplication");
        provenanceaction.setClassname("deduplication");
        provenanceaction.setSchemeid("dnet:provenanceActions");
        provenanceaction.setSchemename("dnet:provenanceActions");

        d.setProvenanceaction(provenanceaction);

        rel.setDataInfo(d);

        return new AtomicAction<>(Relation.class, rel);
    }

    private AtomicAction doTransform(eu.dnetlib.actionmanager.actions.AtomicAction aa)
            throws InvalidProtocolBufferException {
        final OafProtos.Oaf proto_oaf = OafProtos.Oaf.parseFrom(aa.getTargetValue());
        final Oaf oaf = ProtoConverter.convert(proto_oaf);
        switch (proto_oaf.getKind()) {
            case entity:
                switch (proto_oaf.getEntity().getType()) {
                    case datasource:
                        return new AtomicAction<>(Datasource.class, (Datasource) oaf);
                    case organization:
                        return new AtomicAction<>(Organization.class, (Organization) oaf);
                    case project:
                        return new AtomicAction<>(Project.class, (Project) oaf);
                    case result:
                        final String resulttypeid =
                                proto_oaf
                                        .getEntity()
                                        .getResult()
                                        .getMetadata()
                                        .getResulttype()
                                        .getClassid();
                        switch (resulttypeid) {
                            case "publication":
                                return new AtomicAction<>(Publication.class, (Publication) oaf);
                            case "software":
                                return new AtomicAction<>(Software.class, (Software) oaf);
                            case "other":
                                return new AtomicAction<>(
                                        OtherResearchProduct.class, (OtherResearchProduct) oaf);
                            case "dataset":
                                return new AtomicAction<>(Dataset.class, (Dataset) oaf);
                            default:
                                // can be an update, where the resulttype is not specified
                                return new AtomicAction<>(Result.class, (Result) oaf);
                        }
                    default:
                        throw new IllegalArgumentException(
                                "invalid entity type: " + proto_oaf.getEntity().getType());
                }
            case relation:
                return new AtomicAction<>(Relation.class, (Relation) oaf);
            default:
                throw new IllegalArgumentException("invalid kind: " + proto_oaf.getKind());
        }
    }

    private String getTargetBaseDir(String isLookupUrl) throws ISLookUpException {
        ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
        String XQUERY =
                "collection('/db/DRIVER/ServiceResources/ActionManagerServiceResourceType')//SERVICE_PROPERTIES/PROPERTY[@key = 'basePath']/@value/string()";
        return isLookUp.getResourceProfileByQuery(XQUERY);
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession.builder()
                .appName(TransformActions.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
