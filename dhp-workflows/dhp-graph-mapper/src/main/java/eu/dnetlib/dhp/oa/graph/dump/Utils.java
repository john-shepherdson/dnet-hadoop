
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.graph.Constants;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;
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

	public static List<Relation> getRelationPair(String pid1, String pid2, String type1, String type2,
		String semtype, String rel1, String rel2) {
		List<Relation> ret = new ArrayList<>();
		ret
			.add(
				Relation
					.newInstance(
						Node.newInstance(pid1, type1),
						Node.newInstance(pid2, type2),
						RelType.newInstance(rel1, semtype),
						null));

		ret
			.add(
				Relation
					.newInstance(
						Node.newInstance(pid2, type2),
						Node.newInstance(pid1, type1),
						RelType.newInstance(rel2, semtype),
						null));

		return ret;
	}

	public static Entity getEntity(String fund, String code) throws DocumentException {
		{
			final Document doc;
			doc = new SAXReader().read(new StringReader(fund));
			String name = ((org.dom4j.Node) (doc.selectNodes("//funder/shortname").get(0))).getText();
			return Entity.newInstance(name + ":" + code);
		}
	}
}
