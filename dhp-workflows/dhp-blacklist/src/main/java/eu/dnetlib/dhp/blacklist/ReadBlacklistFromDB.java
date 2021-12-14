
package eu.dnetlib.dhp.blacklist;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.common.RelationInverse;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class ReadBlacklistFromDB implements Closeable {

	private final DbClient dbClient;
	private static final Log log = LogFactory.getLog(ReadBlacklistFromDB.class);

	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String QUERY = "SELECT source_type, unnest(original_source_objects) as source, " +
		"target_type, unnest(original_target_objects) as target, " +
		"relationship FROM blacklist WHERE status = 'ACCEPTED'";

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadBlacklistFromDB.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/blacklist/blacklist_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");
		final String hdfsPath = parser.get("hdfsPath") + "/blacklist";
		final String hdfsNameNode = parser.get("hdfsNameNode");

		try (final ReadBlacklistFromDB rbl = new ReadBlacklistFromDB(hdfsPath, hdfsNameNode, dbUrl, dbUser,
			dbPassword)) {

			log.info("Processing blacklist...");
			rbl.execute(QUERY, rbl::processBlacklistEntry);

		}
	}

	public void execute(final String sql, final Function<ResultSet, List<Relation>> producer) {

		final Consumer<ResultSet> consumer = rs -> producer.apply(rs).forEach(r -> writeRelation(r));

		dbClient.processResults(sql, consumer);
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
			RelationInverse ri = ModelSupport.findInverse(encoding);
			direct.setRelClass(ri.getRelClass());
			inverse.setRelClass(ri.getInverseRelClass());
			direct.setRelType(ri.getRelType());
			inverse.setRelType(ri.getRelType());
			direct.setSubRelType(ri.getSubReltype());
			inverse.setSubRelType(ri.getSubReltype());

			return Arrays.asList(direct, inverse);

		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		dbClient.close();
		writer.close();
	}

	public ReadBlacklistFromDB(
		final String hdfsPath, String hdfsNameNode, final String dbUrl, final String dbUser, final String dbPassword)
		throws IOException {

		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);
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
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

}
