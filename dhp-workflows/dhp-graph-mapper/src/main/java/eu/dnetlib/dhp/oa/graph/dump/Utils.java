
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.complete.Constants;
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
				"%s|%s::%s", Constants.CONTEXT_ID, Constants.CONTEXT_NS_PREFIX,
				DHPUtils.md5(id));
	}

	public static CommunityMap getCommunityMap(SparkSession spark, String communityMapPath) {

		return new Gson().fromJson(spark.read().textFile(communityMapPath).collectAsList().get(0), CommunityMap.class);

	}

	public static CommunityMap readCommunityMap(FileSystem fileSystem, String communityMapPath) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(communityMapPath))));
		StringBuffer sb = new StringBuffer();
		try {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} finally {
			br.close();

		}

		return new Gson().fromJson(sb.toString(), CommunityMap.class);
	}

}
