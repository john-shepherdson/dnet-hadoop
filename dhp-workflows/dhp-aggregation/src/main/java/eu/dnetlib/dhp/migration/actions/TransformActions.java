package eu.dnetlib.dhp.migration.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;

public class TransformActions implements Serializable {

    private static final Log log = LogFactory.getLog(TransformActions.class);
    private static final String SEPARATOR = "/";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(MigrateActionSet.class.getResourceAsStream(
                        "/eu/dnetlib/dhp/migration/transform_actionsets_parameters.json")));
        parser.parseArgument(args);

        new TransformActions().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, IOException {

        final String isLookupUrl = parser.get("isLookupUrl");
        log.info("isLookupUrl: " + isLookupUrl);

        final String inputPaths  = parser.get("inputPaths");

        if (StringUtils.isBlank(inputPaths)) {
            throw new RuntimeException("empty inputPaths");
        }
        log.info("inputPaths: " + inputPaths);

        final String targetBaseDir = getTargetBaseDir(isLookupUrl);

        try(SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

            for(String sourcePath : Lists.newArrayList(Splitter.on(",").split(inputPaths))) {

                LinkedList<String> pathQ = Lists.newLinkedList(Splitter.on(SEPARATOR).split(sourcePath));

                final String rawset = pathQ.pollLast();
                final String actionSetDirectory = pathQ.pollLast();

                final Path targetDirectory = new Path(targetBaseDir + SEPARATOR + actionSetDirectory + SEPARATOR + rawset);

                if (fs.exists(targetDirectory)) {
                    log.info(String.format("found target directory '%s", targetDirectory));
                    fs.delete(targetDirectory, true);
                    log.info(String.format("deleted target directory '%s", targetDirectory));
                }

                log.info(String.format("transforming actions from '%s' to '%s'", sourcePath, targetDirectory));

                sc.sequenceFile(sourcePath, Text.class, Text.class)
                    .mapToPair(a -> new Tuple2<>(a._1(), AtomicAction.fromJSON(a._2().toString())))
                    .mapToPair(a -> new Tuple2<>(a._1(), transformAction(a._2())))

                    .saveAsHadoopFile(targetDirectory.toString(), Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
            }
        }
    }

    private Text transformAction(AtomicAction aa) throws InvalidProtocolBufferException, JsonProcessingException {

        final ObjectMapper mapper = new ObjectMapper();
        if (aa.getTargetValue() != null && aa.getTargetValue().length > 0) {
            Oaf oaf = ProtoConverter.convert(OafProtos.Oaf.parseFrom(aa.getTargetValue()));
            aa.setTargetValue(mapper.writeValueAsString(oaf).getBytes());
        }

        return new Text(mapper.writeValueAsString(aa));
    }

    private String getTargetBaseDir(String isLookupUrl) throws ISLookUpException {
        ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
        String XQUERY = "collection('/db/DRIVER/ServiceResources/ActionManagerServiceResourceType')//SERVICE_PROPERTIES/PROPERTY[@key = 'basePath']/@value/string()";
        return isLookUp.getResourceProfileByQuery(XQUERY);
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(TransformActions.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
