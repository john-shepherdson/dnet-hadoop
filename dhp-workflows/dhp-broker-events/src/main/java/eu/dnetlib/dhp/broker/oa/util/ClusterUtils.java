
package eu.dnetlib.dhp.broker.oa.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class ClusterUtils {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private ClusterUtils() {}

	public static void createDirIfMissing(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static void removeDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
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
		return id.contains("dedup");
	}

	public static final boolean isValidResultResultClass(final String s) {
		return ModelConstants.IS_REFERENCED_BY.equals(s)
				|| ModelConstants.IS_RELATED_TO.equals(s)
				|| ModelConstants.REFERENCES.equals(s)
				|| ModelConstants.IS_SUPPLEMENTED_BY.equals(s)
				|| ModelConstants.IS_SUPPLEMENT_TO.equals(s);
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

	public static List<Path> listFiles(final String path, final FileSystem fileSystem, final String suffix) throws FileNotFoundException, IOException {
		final RemoteIterator<LocatedFileStatus> ls = fileSystem.listFiles(new Path(path), false);
		final List<Path> files = new ArrayList<>();
		while (ls.hasNext()) {
			final LocatedFileStatus current = ls.next();
			if (current.getPath().getName().endsWith(suffix)) {
				files.add(current.getPath());
			}
		}
		return files;
	}

	public static Dataset<Relation> loadMergedRelations(final String graphPath, final SparkSession spark) {
		return ClusterUtils
				.readPath(spark, graphPath + "/relation", Relation.class)
				.map((MapFunction<Relation, Relation>) r -> {
					r.setSource(ConversionUtils.cleanOpenaireId(r.getSource()));
					r.setTarget(ConversionUtils.cleanOpenaireId(r.getTarget()));
					return r;
				}, Encoders.bean(Relation.class))
				.filter((FilterFunction<Relation>) r -> ModelConstants.IS_MERGED_IN.equals(r.getRelClass()));
	}

	public static Dataset<Relation> loadRawRelations(final String relationsPath, final String relType, final SparkSession spark) {
		return ClusterUtils
				.readPath(spark, relationsPath, Relation.class)
				.map((MapFunction<Relation, Relation>) r -> {
					r.setSource(ConversionUtils.cleanOpenaireId(r.getSource()));
					r.setTarget(ConversionUtils.cleanOpenaireId(r.getTarget()));
					return r;
				}, Encoders.bean(Relation.class))
				.filter((FilterFunction<Relation>) r -> relType.equals(r.getRelType()));
	}

}
