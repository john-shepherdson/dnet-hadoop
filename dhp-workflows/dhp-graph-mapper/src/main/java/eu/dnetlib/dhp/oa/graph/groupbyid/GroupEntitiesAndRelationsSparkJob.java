
package eu.dnetlib.dhp.oa.graph.groupbyid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.toSeq;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

/**
 * Groups the graph content by entity identifier to ensure ID uniqueness
 */
public class GroupEntitiesSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GroupEntitiesSparkJob.class);

	private final static String ID_JPATH = "$.id";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GroupEntitiesSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/group_graph_entities_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String graphInputPath = parser.get("graphInputPath");
		log.info("graphInputPath: {}", graphInputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				groupEntities(spark, graphInputPath, outputPath);
			});
	}

	private static void groupEntities(
		SparkSession spark,
		String inputPath,
		String outputPath) {

		TypedColumn<Oaf, Oaf> aggregator = new GroupingAggregator().toColumn();
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		spark
			.read()
			.textFile(toSeq(listEntityPaths(inputPath, sc)))
			.map((MapFunction<String, Oaf>) s -> parseEntity(s), Encoders.kryo(Oaf.class))
			.groupByKey((MapFunction<Oaf, String>) oaf -> ModelSupport.idFn().apply(oaf), Encoders.STRING())
			.agg(aggregator)
			.map((MapFunction<Tuple2<String, Oaf>, Oaf>) Tuple2::_2, Encoders.kryo(Oaf.class))
			.write()
			.mode(SaveMode.Overwrite)
			.save(outputPath);
	}

	public static class GroupingAggregator extends Aggregator<Oaf, Oaf, Oaf> {

		@Override
		public Oaf zero() {
			return null;
		}

		@Override
		public Oaf reduce(Oaf b, Oaf a) {
			return mergeAndGet(b, a);
		}

		private Oaf mergeAndGet(Oaf b, Oaf a) {
			if (Objects.nonNull(a) && Objects.nonNull(b)) {
				return OafMapperUtils.merge(b, a);
			}
			return Objects.isNull(a) ? b : a;
		}

		@Override
		public Oaf merge(Oaf b, Oaf a) {
			return mergeAndGet(b, a);
		}

		@Override
		public Oaf finish(Oaf j) {
			return j;
		}

		@Override
		public Encoder<Oaf> bufferEncoder() {
			return Encoders.kryo(Oaf.class);
		}

		@Override
		public Encoder<Oaf> outputEncoder() {
			return Encoders.kryo(Oaf.class);
		}

	}

	private static <T extends Oaf> Oaf parseEntity(String s) {
		String prefix = StringUtils.substringBefore(jPath(ID_JPATH, s), "|");
		try {
			switch (prefix) {
				case "10":
					return OBJECT_MAPPER.readValue(s, Datasource.class);
				case "20":
					return OBJECT_MAPPER.readValue(s, Organization.class);
				case "40":
					return OBJECT_MAPPER.readValue(s, Project.class);
				case "50":
					String resultType = jPath("$.resulttype.classid", s);
					switch (resultType) {
						case "publication":
							return OBJECT_MAPPER.readValue(s, Publication.class);
						case "dataset":
							return OBJECT_MAPPER.readValue(s, eu.dnetlib.dhp.schema.oaf.Dataset.class);
						case "software":
							return OBJECT_MAPPER.readValue(s, Software.class);
						case "other":
							return OBJECT_MAPPER.readValue(s, OtherResearchProduct.class);
						default:
							throw new IllegalArgumentException(String.format("invalid resultType: '%s'", resultType));
					}
				default:
					throw new IllegalArgumentException(String.format("invalid id prefix: '%s'", prefix));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<String> listEntityPaths(String inputPath, JavaSparkContext sc) {
		return HdfsSupport
			.listFiles(inputPath, sc.hadoopConfiguration())
			.stream()
			.filter(p -> !p.contains("relation"))
			.collect(Collectors.toList());
	}

	private static String jPath(final String path, final String json) {
		Object o = JsonPath.read(json, path);
		if (o instanceof String)
			return (String) o;
		throw new IllegalStateException(String.format("could not extract '%s' from:\n%s", path, json));
	}

}
