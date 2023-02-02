
package eu.dnetlib.dhp.oa.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.toSeq;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

/**
 * Groups the graph content by entity identifier to ensure ID uniqueness
 */
public class GroupEntitiesSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GroupEntitiesSparkJob.class);

	private static final String ID_JPATH = "$.id";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GroupEntitiesSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/merge/group_graph_entities_parameters.json"));
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

		final TypedColumn<Entity, Entity> aggregator = new GroupingAggregator().toColumn();
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		spark
			.read()
			.textFile(toSeq(listEntityPaths(inputPath, sc)))
			.map((MapFunction<String, Entity>) GroupEntitiesSparkJob::parseOaf, Encoders.kryo(Entity.class))
			.filter((FilterFunction<Entity>) e -> StringUtils.isNotBlank(ModelSupport.idFn().apply(e)))
			.groupByKey((MapFunction<Entity, String>) oaf -> ModelSupport.idFn().apply(oaf), Encoders.STRING())
			.agg(aggregator)
			.map(
				(MapFunction<Tuple2<String, Entity>, String>) t -> t._2().getClass().getName() +
					"|" + OBJECT_MAPPER.writeValueAsString(t._2()),
				Encoders.STRING())
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.text(outputPath);
	}

	public static class GroupingAggregator extends Aggregator<Entity, Entity, Entity> {

		@Override
		public Entity zero() {
			return null;
		}

		@Override
		public Entity reduce(Entity b, Entity a) {
			return mergeAndGet(b, a);
		}

		private Entity mergeAndGet(Entity b, Entity a) {
			if (Objects.nonNull(a) && Objects.nonNull(b)) {
				return MergeUtils.mergeEntities(b, a);
			}
			return Objects.isNull(a) ? b : a;
		}

		@Override
		public Entity merge(Entity b, Entity a) {
			return mergeAndGet(b, a);
		}

		@Override
		public Entity finish(Entity j) {
			return j;
		}

		@Override
		public Encoder<Entity> bufferEncoder() {
			return Encoders.kryo(Entity.class);
		}

		@Override
		public Encoder<Entity> outputEncoder() {
			return Encoders.kryo(Entity.class);
		}

	}

	private static Entity parseOaf(String s) {

		DocumentContext dc = JsonPath
			.parse(s, Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS));
		final String id = dc.read(ID_JPATH);
		if (StringUtils.isNotBlank(id)) {

			String prefix = StringUtils.substringBefore(id, "|");
			switch (prefix) {
				case "10":
					return parse(s, Datasource.class);
				case "20":
					return parse(s, Organization.class);
				case "40":
					return parse(s, Project.class);
				case "50":
					String resultType = dc.read("$.resulttype.classid");
					switch (resultType) {
						case "publication":
							return parse(s, Publication.class);
						case "dataset":
							return parse(s, eu.dnetlib.dhp.schema.oaf.Dataset.class);
						case "software":
							return parse(s, Software.class);
						case "other":
							return parse(s, OtherResearchProduct.class);
						default:
							throw new IllegalArgumentException(String.format("invalid resultType: '%s'", resultType));
					}
				default:
					throw new IllegalArgumentException(String.format("invalid id prefix: '%s'", prefix));
			}
		} else {
			throw new IllegalArgumentException(String.format("invalid oaf: '%s'", s));
		}
	}

	private static <T extends Entity> Entity parse(String s, Class<T> clazz) {
		try {
			return OBJECT_MAPPER.readValue(s, clazz);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static List<String> listEntityPaths(String inputPath, JavaSparkContext sc) {
		return HdfsSupport
			.listFiles(inputPath, sc.hadoopConfiguration())
			.stream()
			.filter(f -> !f.toLowerCase().contains("relation"))
			.collect(Collectors.toList());
	}

}
