
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.mdstore.MDStoreWithInfo;

public class MigrateHdfsMdstoresApplication extends AbstractMigrationApplication {

	private static final Logger log = LoggerFactory.getLogger(MigrateHdfsMdstoresApplication.class);

	private final String mdstoreManagerUrl;

	private final String format;

	private final String layout;

	private final String interpretation;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(MigrateHdfsMdstoresApplication.class
					.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_hdfs_mstores_parameters.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String mdstoreManagerUrl = parser.get("mdstoreManagerUrl");
		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");

		final SparkConf conf = new SparkConf();
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			try (final MigrateHdfsMdstoresApplication app =
				new MigrateHdfsMdstoresApplication(hdfsPath, mdstoreManagerUrl, mdFormat, mdLayout, mdInterpretation)) {
				app.execute(spark);
			}
		});
	}

	public MigrateHdfsMdstoresApplication(final String hdfsPath, final String mdstoreManagerUrl, final String format, final String layout,
		final String interpretation) throws Exception {
		super(hdfsPath);
		this.mdstoreManagerUrl = mdstoreManagerUrl;
		this.format = format;
		this.layout = layout;
		this.interpretation = interpretation;
	}

	public void execute(final SparkSession spark) throws Exception {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		final Set<String> paths = mdstorePaths(sc);
		log.info("Found " + paths.size() + " not empty mdstores");

		spark.read()
			.parquet(paths.toArray(new String[paths.size()]))
			.map((MapFunction<Row, String>) r -> r.getAs("body"), Encoders.STRING())
			.foreach(xml -> emit(xml, String.format("%s-%s-%s", format, layout, interpretation)));
	}

	private Set<String> mdstorePaths(final JavaSparkContext sc) throws Exception {
		final String url = mdstoreManagerUrl + "/mdstores";
		final ObjectMapper objectMapper = new ObjectMapper();

		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				final String json = IOUtils.toString(response.getEntity().getContent());
				final MDStoreWithInfo[] mdstores = objectMapper.readValue(json, MDStoreWithInfo[].class);
				return Arrays.stream(mdstores)
					.filter(md -> md.getFormat().equalsIgnoreCase(format))
					.filter(md -> md.getLayout().equalsIgnoreCase(layout))
					.filter(md -> md.getInterpretation().equalsIgnoreCase(interpretation))
					.filter(md -> StringUtils.isNotBlank(md.getHdfsPath()))
					.filter(md -> StringUtils.isNotBlank(md.getCurrentVersion()))
					.filter(md -> md.getSize() > 0)
					.map(md -> md.getHdfsPath() + "/" + md.getCurrentVersion() + "/store")
					.filter(p -> HdfsSupport.exists(p, sc.hadoopConfiguration()))
					.collect(Collectors.toSet());
			}
		}
	}

}
