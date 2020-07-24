
package eu.dnetlib.dhp.oa.graph.dump;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Constants;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class Utils {
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	public static ISLookUpService getIsLookUpService(String isLookUpUrl) {
		return ISLookupClientFactory.getLookUpService(isLookUpUrl);
	}

	public static String getContextId(String id) {

		return String
			.format(
				"%s|%s::%s", eu.dnetlib.dhp.schema.dump.oaf.graph.Constants.CONTEXT_ID, Constants.CONTEXT_NS_PREFIX,
				DHPUtils.md5(id));
	}
}
