
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.dhp.schema.orcid.Work;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;

public class SparkUpdateOrcidDatasets {

	public static void main(String[] args) throws IOException, Exception {
		Logger logger = LoggerFactory.getLogger(SparkUpdateOrcidDatasets.class);
		logger.info("[ SparkUpdateOrcidDatasets STARTED]");

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateOrcidDatasets.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: ", workingPath);
//		final String outputPath = parser.get("outputPath");
//		logger.info("outputPath: ", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				JavaPairRDD<Text, Text> xmlSummariesRDD = sc
					.sequenceFile(workingPath.concat("xml/authors/xml_authors.seq"), Text.class, Text.class);
				xmlSummariesRDD
					.map(seq -> {
						AuthorSummary authorSummary = XMLRecordParser
							.VTDParseAuthorSummary(seq._2().toString().getBytes());
						authorSummary
							.setBase64CompressData(ArgumentApplicationParser.compressArgument(seq._2().toString()));
						return authorSummary;
					})
					.filter(authorSummary -> authorSummary != null)
					.map(authorSummary -> JsonWriter.create(authorSummary))
					.saveAsTextFile(workingPath.concat("orcid_dataset/authors"), GzipCodec.class);

				JavaPairRDD<Text, Text> xmlWorksRDD = sc
					.sequenceFile(workingPath.concat("xml/works/*"), Text.class, Text.class);

				xmlWorksRDD
					.map(seq -> {
						WorkDetail workDetail = XMLRecordParserNoDoi.VTDParseWorkData(seq._2().toString().getBytes());
						Work work = new Work();
						work.setWorkDetail(workDetail);
						work.setBase64CompressData(ArgumentApplicationParser.compressArgument(seq._2().toString()));
						return work;
					})
					.filter(work -> work != null)
					.map(work -> JsonWriter.create(work))
					.saveAsTextFile(workingPath.concat("orcid_dataset/works"), GzipCodec.class);
			});

	}
}
