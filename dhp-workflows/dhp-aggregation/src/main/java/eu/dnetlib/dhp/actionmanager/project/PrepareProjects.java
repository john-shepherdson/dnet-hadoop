
package eu.dnetlib.dhp.actionmanager.project;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.common.RelationInverse;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareProjects implements Closeable {
	private static final Log log = LogFactory.getLog(PrepareProjects.class);
	private final Configuration conf;
	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final HttpConnector httpConnector;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareProjects.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");

		try (final PrepareProjects prepareProjects = new PrepareProjects(hdfsPath, hdfsNameNode)) {

			log.info("Getting projects...");
			prepareProjects.execute(fileURL);

		}
	}

	public void execute(final String fileURL) throws Exception {

		String projects = httpConnector.getInputSource(fileURL);
		final CSVFormat format = CSVFormat.EXCEL
				.withHeader()
				.withDelimiter(';')
				.withQuote('"')
				.withTrim();
		final CSVParser parser = CSVParser.parse(projects, format);
		final Set<String> headers = parser.getHeaderMap().keySet();
	}

	public List<Relation> processBlacklistEntry(ResultSet rs) {
		try {
			Relation direct = new Relation();
			Relation inverse = new Relation();

			String source_prefix = ModelSupport.entityIdPrefix.get(rs.getString("source_type"));
			String target_prefix = ModelSupport.entityIdPrefix.get(rs.getString("target_type"));

			String source_direct = source_prefix + "|" + rs.getString("source");
			direct.setSource(source_direct);
			inverse.setTarget(source_direct);

			String target_direct = target_prefix + "|" + rs.getString("target");
			direct.setTarget(target_direct);
			inverse.setSource(target_direct);

			String encoding = rs.getString("relationship");
			RelationInverse ri = ModelSupport.relationInverseMap.get(encoding);
			direct.setRelClass(ri.getRelation());
			inverse.setRelClass(ri.getInverse());
			direct.setRelType(ri.getRelType());
			inverse.setRelType(ri.getRelType());
			direct.setSubRelType(ri.getSubReltype());
			inverse.setSubRelType(ri.getSubReltype());

			return Arrays.asList(direct, inverse);

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public PrepareProjects(
		final String hdfsPath, String hdfsNameNode)
		throws Exception {

		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);
		this.httpConnector = new HttpConnector();
		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}

		this.writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

	}

	protected void writeRelation(final Relation r) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(r));
			writer.newLine();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

}
