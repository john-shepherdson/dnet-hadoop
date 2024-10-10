
package eu.dnetlib.dhp.oa.dedup.local;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.spark_project.guava.hash.Hashing;

import com.cloudera.com.fasterxml.jackson.core.JsonFactory;
import com.cloudera.com.fasterxml.jackson.databind.JsonNode;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.pace.config.DedupConfig;
import scala.collection.JavaConverters;
import scala.collection.convert.Wrappers;
import scala.collection.mutable.ArrayBuffer;

public abstract class DedupLocalTestUtils {

	public static String prepareTable(Row doc) {
		StringBuilder ret = new StringBuilder("<table>");

		for (String fieldName : doc.schema().fieldNames()) {
			Object value = doc.getAs(fieldName);
			if (value.getClass() == String.class) {
				ret.append("<tr><th>").append(fieldName).append("</th><td>").append(value).append("</td></tr>");
			} else if (value.getClass() == Wrappers.JListWrapper.class) {
				List<String> values = IteratorUtils
					.toList(
						JavaConverters
							.asJavaIteratorConverter(((Wrappers.JListWrapper<String>) value).iterator())
							.asJava())
					.stream()
					.map(DedupLocalTestUtils::takeValue)
					.collect(Collectors.toList());
				ret
					.append("<tr><th>")
					.append(fieldName)
					.append("</th><td>[")
					.append(String.join(";", values))
					.append("]</td></tr>");
			} else if (value.getClass() == ArrayBuffer.class) {
				List<String> values = new ArrayList<>(IteratorUtils
					.toList(JavaConverters.asJavaIteratorConverter(((ArrayBuffer<String>) value).iterator()).asJava()));
				ret
					.append("<tr><th>")
					.append(fieldName)
					.append("</th><td>[")
					.append(String.join(";", values))
					.append("]</td></tr>");
			}

		}

		ret.append("</table>");
		return ret.toString();

	}

	protected static String fileToString(String filePath) throws IOException {

		Path path = new Path(filePath);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		try {
			return String.join("", br.lines().collect(Collectors.toList()));
		} finally {
			br.close();
		}
	}

	public static void prepareGraphParams(Dataset<Row> entities, Dataset<Relation> simRels, String filePath,
		String templateFilePath) {

		List<String> vertexes = entities.toJavaRDD().map(r -> r.getAs("identifier").toString()).collect();

		List<Node> nodes = entities
			.toJavaRDD()
			.map(
				e -> new Node(e.getAs("identifier").toString(), vertexes.indexOf(e.getAs("identifier").toString()),
					prepareTable(e)))
			.collect();

		List<Edge> edges = simRels
			.toJavaRDD()
			.collect()
			.stream()
			.map(sr -> new Edge(vertexes.indexOf(sr.getSource()), vertexes.indexOf(sr.getTarget())))
			.collect(Collectors.toList());

		try (FileWriter fw = new FileWriter(filePath)) {
			String fullText = IOUtils.toString(new FileReader(templateFilePath));

			String s = fullText
				.replaceAll("%nodes%", new ObjectMapper().writeValueAsString(nodes))
				.replaceAll("%edges%", new ObjectMapper().writeValueAsString(edges));

			IOUtils.write(s, fw);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static long hash(final String id) {
		return Hashing.murmur3_128().hashString(id).asLong();
	}

	public static Relation createRel(String source, String target, String relClass, DedupConfig dedupConf) {

		String entityType = dedupConf.getWf().getEntityType();

		Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setRelClass(relClass);
		r.setRelType(entityType + entityType.substring(0, 1).toUpperCase() + entityType.substring(1));
		r.setSubRelType(ModelConstants.DEDUP);
		return r;
	}

	public static OafEntity createOafEntity(String id, OafEntity base, long ts) {
		try {
			OafEntity res = (OafEntity) BeanUtils.cloneBean(base);
			res.setId(id);
			res.setLastupdatetimestamp(ts);
			return res;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String takeValue(String json) {
		ObjectMapper mapper = new ObjectMapper(new JsonFactory());
		try {
			JsonNode rootNode = mapper.readTree(json);
			return rootNode.get("value").toString().replaceAll("\"", "");

		} catch (Exception e) {
			return json;
		}

	}

}

class Node implements Serializable {
	String label;
	int id;
	String title;

	public Node(String label, int id, String title) {
		this.label = label;
		this.id = id;
		this.title = title;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
}

class Edge implements Serializable {
	int from;
	int to;

	public Edge(int from, int to) {
		this.from = from;
		this.to = to;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getTo() {
		return to;
	}

	public void setTo(int to) {
		this.to = to;
	}
}
