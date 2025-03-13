
package eu.dnetlib.dhp.oozie;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class RunSQLSparkJob {
	private static final Logger log = LoggerFactory.getLogger(RunSQLSparkJob.class);

	private final ArgumentApplicationParser parser;

	public RunSQLSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {

		Map<String, String> params = new HashMap<>();
		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].startsWith("--")) {
				params.put(args[i].substring(2), args[++i]);
			}
		}

		/*
		 * String jsonConfiguration = IOUtils .toString( Objects .requireNonNull( RunSQLSparkJob.class
		 * .getResourceAsStream( "/eu/dnetlib/dhp/oozie/run_sql_parameters.json"))); final ArgumentApplicationParser
		 * parser = new ArgumentApplicationParser(jsonConfiguration); parser.parseArgument(args);
		 */

		Boolean isSparkSessionManaged = Optional
			.ofNullable(params.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		URL url = com.google.common.io.Resources.getResource(params.get("sql"));
		String raw_sql = Resources.toString(url, StandardCharsets.UTF_8);

		String sql = StringSubstitutor.replace(raw_sql, params);
		log.info("sql: {}", sql);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", params.get("hiveMetastoreUris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				for (String statement : sql.split(";\\s*/\\*\\s*EOS\\s*\\*/\\s*")) {
					log.info("executing: {}", statement);
					long startTime = System.currentTimeMillis();
					try {
						spark.sql(statement).show();
					} catch (Exception e) {
						log.error("Error executing statement: {}", statement, e);
						System.err.println("Error executing statement: " + statement + "\n" + e);
						throw e;
					}
					log
						.info(
							"executed in {}",
							DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.S"));
				}
			});
	}

}
