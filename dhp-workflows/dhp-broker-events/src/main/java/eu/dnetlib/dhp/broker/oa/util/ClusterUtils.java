
package eu.dnetlib.dhp.broker.oa.util;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.HdfsSupport;

public class ClusterUtils {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
		return id.contains("dedup_wf_");
	}

	public static final boolean isValidResultResultClass(final String s) {
		return s.equals("isReferencedBy")
			|| s.equals("isRelatedTo")
			|| s.equals("references")
			|| s.equals("isSupplementedBy")
			|| s.equals("isSupplementedTo");
	}

}
