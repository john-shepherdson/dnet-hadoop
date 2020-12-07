
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;
import scala.Tuple2;

public class SparkUpdateOrcidDatasets {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

				sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

				JavaPairRDD<Text, Text> xmlSummariesRDD = sc
					.sequenceFile(workingPath.concat("xml/authors/xml_authors.seq"), Text.class, Text.class);
				xmlSummariesRDD
					.repartition(5)
					.map(seq -> XMLRecordParser.VTDParseAuthorData(seq._2().toString().getBytes()))
					.filter(summary -> summary != null)
					.mapToPair(
						summary -> new Tuple2<>(summary.getOid(),
							OBJECT_MAPPER.writeValueAsString(summary)))
					.mapToPair(t -> new Tuple2(new Text(t._1()), new Text(t._2())))
					.saveAsNewAPIHadoopFile(
						workingPath.concat("orcid_dataset/authors"),
						Text.class,
						Text.class,
						SequenceFileOutputFormat.class,
						sc.hadoopConfiguration());

				JavaPairRDD<Text, Text> xmlWorksRDD = sc
					.sequenceFile(workingPath.concat("xml/works/*"), Text.class, Text.class);

				xmlWorksRDD
					.map(seq -> XMLRecordParserNoDoi.VTDParseWorkData(seq._2().toString().getBytes()))
					.filter(work -> work != null)
					.mapToPair(
						work -> new Tuple2<>(work.getOid().concat("_").concat(work.getId()),
							OBJECT_MAPPER.writeValueAsString(work)))
					.mapToPair(t -> new Tuple2(new Text(t._1()), new Text(t._2())))
					.saveAsNewAPIHadoopFile(
						workingPath.concat("orcid_dataset/works"),
						Text.class,
						Text.class,
						SequenceFileOutputFormat.class,
						sc.hadoopConfiguration());
			});

	}

	private static AuthorData loadAuthorFromJson(Text orcidId, Text json) {
		AuthorData authorData = new AuthorData();
		authorData.setOid(orcidId.toString());
		JsonElement jElement = new JsonParser().parse(json.toString());
		authorData.setName(getJsonValue(jElement, "name"));
		authorData.setSurname(getJsonValue(jElement, "surname"));
		authorData.setCreditName(getJsonValue(jElement, "creditname"));
		return authorData;
	}

	private static WorkData loadWorkFromJson(Text orcidId, Text json) {
		WorkData workData = new WorkData();
		workData.setOid(orcidId.toString());
		JsonElement jElement = new JsonParser().parse(json.toString());
		workData.setDoi(getJsonValue(jElement, "doi"));
		return workData;
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return null;
	}
}
