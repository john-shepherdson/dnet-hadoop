
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class ClusterUtils {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void createDirIfMissing(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static void removeDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static Dataset<Relation> loadRelations(final String graphPath, final SparkSession spark) {
		return ClusterUtils
			.readPath(spark, graphPath + "/relation", Relation.class)
			.map((MapFunction<Relation, Relation>) r -> {
				r.setSource(ConversionUtils.cleanOpenaireId(r.getSource()));
				r.setTarget(ConversionUtils.cleanOpenaireId(r.getTarget()));
				return r;
			}, Encoders.bean(Relation.class));
	}

	public static <R> Dataset<R> readPath(
		final SparkSession spark,
		final String inputPath,
		final Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	public static boolean isDedupRoot(final String id) {
		return id.contains("dedup_wf_");
	}

	public static final boolean isValidResultResultClass(final String s) {
		return s.equals("isReferencedBy")
			|| s.equals("isRelatedTo")
			|| s.equals("references")
			|| s.equals("isSupplementedBy")
			|| s.equals("isSupplementedTo");
	}

	public static <T> T incrementAccumulator(final T o, final LongAccumulator acc) {
		if (acc != null) {
			acc.add(1);
		}
		return o;
	}

	public static <T> void save(final Dataset<T> dataset,
		final String path,
		final Class<T> clazz,
		final LongAccumulator acc) {
		dataset
			.map((MapFunction<T, T>) o -> ClusterUtils.incrementAccumulator(o, acc), Encoders.bean(clazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(path);
	}

	public static Set<String> parseParamAsList(final ArgumentApplicationParser parser, final String key) {
		final String s = parser.get(key).trim();

		final Set<String> res = new HashSet<>();

		if (s.length() > 1) { // A value of a single char (for example: '-') indicates an empty list
			Arrays
				.stream(s.split(","))
				.map(String::trim)
				.filter(StringUtils::isNotBlank)
				.forEach(res::add);
		}

		return res;
	}

}
