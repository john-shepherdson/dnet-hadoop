
package eu.dnetlib.dhp.bulktag.eosc;

import static eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory.*;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;

public class ReadMasterDatasourceFromDB implements Closeable {

	private final DbClient dbClient;
	private static final Log log = LogFactory.getLog(ReadMasterDatasourceFromDB.class);

	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String QUERY = "SELECT dso.id datasource, d.id master FROM " +
		"(SELECT id FROM dsm_services WHERE id like 'eosc%') dso " +
		"FULL JOIN  " +
		"(SELECT id, duplicate FROM dsm_dedup_services WHERE duplicate like 'eosc%')d " +
		"ON dso.id = d.duplicate";

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadMasterDatasourceFromDB.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/bulktag/datasourcemaster_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");

		try (
			final ReadMasterDatasourceFromDB rmd = new ReadMasterDatasourceFromDB(hdfsPath, hdfsNameNode, dbUrl, dbUser,
				dbPassword)) {

			log.info("Processing datasources...");
			rmd.execute(QUERY, rmd::datasourceMasterMap);

		}
	}

	public void execute(final String sql, final Function<ResultSet, DatasourceMaster> producer) {

		dbClient.processResults(sql, rs -> writeMap(producer.apply(rs)));
	}

	public DatasourceMaster datasourceMasterMap(ResultSet rs) {
		try {
			DatasourceMaster dm = new DatasourceMaster();
			String datasource = rs.getString("datasource");
			dm.setDatasource(datasource);
			String master = rs.getString("master");
			if (StringUtils.isNotBlank(master))
				dm.setMaster(createOpenaireId(10, master, true));
			else
				dm.setMaster(createOpenaireId(10, datasource, true));
			return dm;

		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		dbClient.close();
		writer.close();
	}

	public ReadMasterDatasourceFromDB(
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

	protected void writeMap(final DatasourceMaster dm) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(dm));
			writer.newLine();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

}
