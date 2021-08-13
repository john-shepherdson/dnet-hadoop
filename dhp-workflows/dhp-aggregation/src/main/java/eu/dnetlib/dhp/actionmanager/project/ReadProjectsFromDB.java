
package eu.dnetlib.dhp.actionmanager.project;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;

/**
 * queries the OpenAIRE database to get the grant agreement of projects collected from corda__h2020. The code collected
 * are written on hdfs using the ProjectSubset model
 */
public class ReadProjectsFromDB implements Closeable {

	private static final Log log = LogFactory.getLog(ReadProjectsFromDB.class);

	private static final String query = "SELECT code  " +
		"from projects where id like 'corda__h2020%' ";

	private final DbClient dbClient;
	private final Configuration conf;
	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadProjectsFromDB.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/read_projects_db.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");

		try (final ReadProjectsFromDB rbl = new ReadProjectsFromDB(hdfsPath, hdfsNameNode, dbUrl, dbUser,
			dbPassword)) {

			log.info("Processing projects...");
			rbl.execute(query, rbl::processProjectsEntry);

		}
	}

	public void execute(final String sql, final Function<ResultSet, List<ProjectSubset>> producer) {

		final Consumer<ResultSet> consumer = rs -> producer.apply(rs).forEach(this::writeProject);

		dbClient.processResults(sql, consumer);
	}

	public List<ProjectSubset> processProjectsEntry(ResultSet rs) {
		try {
			ProjectSubset p = new ProjectSubset();
			p.setCode(rs.getString("code"));
			return Arrays.asList(p);

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void writeProject(final ProjectSubset r) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(r));
			writer.newLine();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public ReadProjectsFromDB(
		final String hdfsPath, String hdfsNameNode, final String dbUrl, final String dbUser, final String dbPassword)
		throws IOException {

		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);

		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		FSDataOutputStream fos = fileSystem.create(hdfsWritePath);

		this.writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
	}

	@Override
	public void close() throws IOException {
		dbClient.close();
		writer.close();
	}
}
