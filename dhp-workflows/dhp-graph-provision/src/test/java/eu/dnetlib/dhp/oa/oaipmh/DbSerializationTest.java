
package eu.dnetlib.dhp.oa.oaipmh;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class DbSerializationTest {

	private static SparkSession spark;

	public static final String dbUrl = "jdbc:postgresql://localhost:5432/db_test";
	public static final String dbUser = null;
	public static final String dbPwd = null;

	@BeforeAll
	public static void beforeAll() throws IOException {

		final SparkConf conf = new SparkConf();
		conf.setAppName("TEST");
		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");

		spark = SparkSession
				.builder()
				.appName("TEST")
				.config(conf)
				.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		spark.stop();
	}

	@Test
	public void testDatabaseSerialization() throws Exception {
		final Properties connectionProperties = new Properties();
		if (dbUser != null) {
			connectionProperties.put("user", dbUser);
		}
		if (dbPwd != null) {
			connectionProperties.put("password", dbPwd);
		}

		runWithSparkSession(new SparkConf(), false, spark -> {

			final List<OaiRecordWrapper> list = new ArrayList<>();

			for (int i = 0; i < 10; i++) {
				final OaiRecordWrapper r = new OaiRecordWrapper();
				r.setId("record_" + i);
				r.setBody("jsahdjkahdjahdajad".getBytes());
				r.setDate(LocalDateTime.now().toString());
				r.setSets(Arrays.asList());
				list.add(r);
			}

			final Dataset<OaiRecordWrapper> docs = spark.createDataset(list, Encoders.bean(OaiRecordWrapper.class));

			docs
					.write()
					.mode(SaveMode.Overwrite)
					.jdbc(dbUrl, IrishOaiExporterJob.TMP_OAI_TABLE, connectionProperties);

		});

		try (final Connection con = DriverManager.getConnection(dbUrl, dbUser, dbPwd)) {
			try (final Statement st = con.createStatement()) {
				final String query = IOUtils.toString(getClass().getResourceAsStream("oai-finalize.sql"));
				st.execute(query);
			}
		}

	}

}
