package eu.dnetlib.dhp.transformation;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.GenerateNativeStoreSparkJob;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.vocabulary.Vocabulary;
import eu.dnetlib.dhp.transformation.vocabulary.VocabularyHelper;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;
import eu.dnetlib.message.MessageType;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TransformSparkJobNode {



    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(TransformSparkJobNode.class.getResourceAsStream("/eu/dnetlib/dhp/transformation/transformation_input_parameters.json")));

        parser.parseArgument(args);

        final String inputPath              = parser.get("input");
        final String outputPath             = parser.get("output");
        final String workflowId             = parser.get("workflowId");
        final String trasformationRule      = extractXSLTFromTR(Objects.requireNonNull(DHPUtils.decompressString(parser.get("transformationRule"))));
        final String master                 = parser.get("master");
        final String rabbitUser 			= parser.get("rabbitUser");
        final String rabbitPassword		    = parser.get("rabbitPassword");
        final String rabbitHost 			= parser.get("rabbitHost");
        final String rabbitReportQueue  	= parser.get("rabbitReportQueue");
        final long dateOfCollection         = new Long(parser.get("dateOfCollection"));
        final boolean test                  = parser.get("isTest") == null?false: Boolean.valueOf(parser.get("isTest"));

        final SparkSession spark = SparkSession
                .builder()
                .appName("TransformStoreSparkJob")
                .master(master)
                .getOrCreate();





        final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
        final Dataset<MetadataRecord> mdstoreInput = spark.read().format("parquet").load(inputPath).as(encoder);
        final LongAccumulator totalItems = spark.sparkContext().longAccumulator("TotalItems");
        final LongAccumulator errorItems = spark.sparkContext().longAccumulator("errorItems");
        final LongAccumulator transformedItems = spark.sparkContext().longAccumulator("transformedItems");
        final Map<String, Vocabulary> vocabularies = new HashMap<>();
        vocabularies.put("dnet:languages", VocabularyHelper.getVocabularyFromAPI("dnet:languages"));
        final TransformFunction transformFunction = new TransformFunction(totalItems, errorItems, transformedItems, trasformationRule, dateOfCollection, vocabularies) ;
        mdstoreInput.map(transformFunction, encoder).write().format("parquet").save(outputPath);
        if (rabbitHost != null) {
            System.out.println("SEND FINAL REPORT");
            final Map<String, String> reportMap = new HashMap<>();
            reportMap.put("inputItem" , ""+ totalItems.value());
            reportMap.put("invalidRecords", "" + errorItems.value());
            reportMap.put("mdStoreSize", "" + transformedItems.value());
            System.out.println(new Message(workflowId, "Transform", MessageType.REPORT, reportMap));
            if (!test) {
                final MessageManager manager = new MessageManager(rabbitHost, rabbitUser, rabbitPassword, false, false, null);
                manager.sendMessage(new Message(workflowId, "Transform", MessageType.REPORT, reportMap), rabbitReportQueue, true, false);
                manager.close();
            }
        }
    }


    private static String extractXSLTFromTR(final String tr) throws DocumentException {
        SAXReader reader = new SAXReader();
        Document document = reader.read(new ByteArrayInputStream(tr.getBytes()));
        Node node = document.selectSingleNode("//CODE/*[local-name()='stylesheet']");
        return node.asXML();
    }
}