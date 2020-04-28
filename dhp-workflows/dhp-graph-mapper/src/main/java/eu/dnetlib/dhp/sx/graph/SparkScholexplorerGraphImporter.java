
package eu.dnetlib.dhp.sx.graph;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.sx.graph.parser.DatasetScholexplorerParser;
import eu.dnetlib.dhp.sx.graph.parser.PublicationScholexplorerParser;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import scala.Tuple2;

/**
 * This Job read a sequential File containing XML stored in the aggregator and generates an RDD of heterogeneous
 * entities like Dataset, Relation, Publication and Unknown
 */
public class SparkScholexplorerGraphImporter {

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkScholexplorerGraphImporter.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/sx/graph/argumentparser/input_graph_scholix_parameters.json")));

		parser.parseArgument(args);
		final SparkSession spark = SparkSession
			.builder()
			.appName(SparkScholexplorerGraphImporter.class.getSimpleName())
			.master(parser.get("master"))
			.getOrCreate();
		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		final String inputPath = parser.get("sourcePath");

		RelationMapper relationMapper = RelationMapper.load();

		sc
			.sequenceFile(inputPath, IntWritable.class, Text.class)
			.map(Tuple2::_2)
			.map(Text::toString)
			.repartition(500)
			.flatMap(
				(FlatMapFunction<String, Oaf>) record -> {
					switch (parser.get("entity")) {
						case "dataset":
							final DatasetScholexplorerParser d = new DatasetScholexplorerParser();
							return d.parseObject(record, relationMapper).iterator();
						case "publication":
							final PublicationScholexplorerParser p = new PublicationScholexplorerParser();
							return p.parseObject(record, relationMapper).iterator();
						default:
							throw new IllegalArgumentException("wrong values of entities");
					}
				})
			.map(
				k -> {
					ObjectMapper mapper = new ObjectMapper();
					return mapper.writeValueAsString(k);
				})
			.saveAsTextFile(parser.get("targetPath"), GzipCodec.class);
	}
}
