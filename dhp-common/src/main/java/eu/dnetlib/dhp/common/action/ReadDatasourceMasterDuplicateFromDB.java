
package eu.dnetlib.dhp.common.action;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.action.model.MasterDuplicate;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class ReadDatasourceMasterDuplicateFromDB {

	private static final Logger log = LoggerFactory.getLogger(ReadDatasourceMasterDuplicateFromDB.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String QUERY = "SELECT distinct dd.id as masterId, d.officialname as masterName, dd.duplicate as duplicateId "
		+
		"FROM dsm_dedup_services dd join dsm_services d on (dd.id = d.id);";

	public static int execute(String dbUrl, String dbUser, String dbPassword, String hdfsPath, String hdfsNameNode)
		throws IOException {
		int count = 0;
		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfsNameNode);
			FileSystem fileSystem = FileSystem.get(conf);
			FSDataOutputStream fos = fileSystem.create(new Path(hdfsPath));

			log.info("running query: {}", QUERY);
			log.info("storing results in: {}", hdfsPath);

			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
				dbClient.processResults(QUERY, rs -> writeMap(datasourceMasterMap(rs), writer));
				count++;
			}
		}
		return count;
	}

	private static MasterDuplicate datasourceMasterMap(ResultSet rs) {
		try {
			final MasterDuplicate md = new MasterDuplicate();

			final String duplicateId = rs.getString("duplicateId");
			final String masterId = rs.getString("masterId");
			final String masterName = rs.getString("masterName");

			md.setDuplicateId(OafMapperUtils.createOpenaireId(10, duplicateId, true));
			md.setMasterId(OafMapperUtils.createOpenaireId(10, masterId, true));
			md.setMasterName(masterName);

			return md;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void writeMap(final MasterDuplicate dm, final BufferedWriter writer) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(dm));
			writer.newLine();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

}
