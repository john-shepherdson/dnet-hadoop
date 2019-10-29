package eu.dnetlib.dhp.collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.model.mdstore.Provenance;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;
import eu.dnetlib.message.MessageType;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GenerateNativeStoreSparkJob {


    public static MetadataRecord parseRecord (final String input, final String xpath, final String encoding, final Provenance provenance, final Long dateOfCollection, final LongAccumulator totalItems, final LongAccumulator invalidRecords)  {

        if(totalItems != null)
            totalItems.add(1);
        try {
            SAXReader reader = new SAXReader();
            Document document = reader.read(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
            Node node = document.selectSingleNode(xpath);
            final String originalIdentifier = node.getText();
            if (StringUtils.isBlank(originalIdentifier)) {
                if (invalidRecords!= null)
                    invalidRecords.add(1);
                return null;
            }
            return new MetadataRecord(originalIdentifier, encoding, provenance, input, dateOfCollection);
        } catch (Throwable e) {
            if (invalidRecords!= null)
                invalidRecords.add(1);
            e.printStackTrace();
            return null;

        }
    }

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(GenerateNativeStoreSparkJob.class.getResourceAsStream("/eu/dnetlib/dhp/collection/collection_input_parameters.json")));
        parser.parseArgument(args);
        final ObjectMapper jsonMapper       = new ObjectMapper();
        final Provenance provenance         = jsonMapper.readValue(parser.get("provenance"), Provenance.class);
        final long dateOfCollection         = new Long(parser.get("dateOfCollection"));

        final SparkSession spark = SparkSession
                .builder()
                .appName("GenerateNativeStoreSparkJob")
                .master(parser.get("master"))
                .getOrCreate();

        final Map<String, String> ongoingMap = new HashMap<>();
        final Map<String, String> reportMap = new HashMap<>();

        final boolean test = parser.get("isTest") == null ? false : Boolean.valueOf(parser.get("isTest"));

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final JavaPairRDD<IntWritable, Text> inputRDD = sc.sequenceFile(parser.get("input"), IntWritable.class, Text.class);

        final LongAccumulator totalItems = sc.sc().longAccumulator("TotalItems");

        final LongAccumulator invalidRecords = sc.sc().longAccumulator("InvalidRecords");

        final MessageManager manager = new MessageManager(parser.get("rabbitHost"), parser.get("rabbitUser"), parser.get("rabbitPassword"), false, false, null);

        final JavaRDD<MetadataRecord> mappeRDD = inputRDD.map(item -> parseRecord(item._2().toString(), parser.get("xpath"), parser.get("encoding"), provenance, dateOfCollection, totalItems, invalidRecords))
                .filter(Objects::nonNull).distinct();

        ongoingMap.put("ongoing", "0");
        if (!test) {
            manager.sendMessage(new Message(parser.get("workflowId"),"DataFrameCreation", MessageType.ONGOING, ongoingMap ), parser.get("rabbitOngoingQueue"), true, false);
        }


        final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
        final Dataset<MetadataRecord> mdstore = spark.createDataset(mappeRDD.rdd(), encoder);
        final LongAccumulator mdStoreRecords = sc.sc().longAccumulator("MDStoreRecords");
        mdStoreRecords.add(mdstore.count());
        ongoingMap.put("ongoing", ""+ totalItems.value());
        if (!test) {
            manager.sendMessage(new Message(parser.get("workflowId"), "DataFrameCreation", MessageType.ONGOING, ongoingMap), parser.get("rabbitOngoingQueue"), true, false);

        }
        mdstore.write().format("parquet").save(parser.get("output"));
        reportMap.put("inputItem" , ""+ totalItems.value());
        reportMap.put("invalidRecords", "" + invalidRecords.value());
        reportMap.put("mdStoreSize", "" + mdStoreRecords.value());
        if (!test) {
            manager.sendMessage(new Message(parser.get("workflowId"), "Collection", MessageType.REPORT, reportMap), parser.get("rabbitReportQueue"), true, false);
            manager.close();
        }

    }
}
