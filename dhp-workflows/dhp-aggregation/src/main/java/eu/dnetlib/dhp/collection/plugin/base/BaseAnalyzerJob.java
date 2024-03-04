
package eu.dnetlib.dhp.collection.plugin.base;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import scala.Tuple2;

public class BaseAnalyzerJob {

	private static final String BASE_DUMP = "BASE_DUMP";
	private static final Logger log = LoggerFactory.getLogger(BaseAnalyzerJob.class);

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
			.toString(
				BaseAnalyzerJob.class
					.getResourceAsStream("/eu/dnetlib/dhp/collection/plugin/base/action_set_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String dataPath = parser.get("dataPath");
		log.info("dataPath {}: ", dataPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String opendoarPath = parser.get("opendoarPath");
		log.info("opendoarPath {}: ", opendoarPath);

		final String typesReportPath = parser.get("typesReportPath");
		log.info("typesReportPath {}: ", typesReportPath);

		final int fromStep = Integer.parseInt(parser.get("fromStep"));
		log.info("fromStep {}: ", fromStep);

		final String dbUrl = parser.get("postgresUrl");
		log.info("postgresUrl {}: ", dbUrl);

		final String dbUser = parser.get("postgresUser");
		log.info("postgresUser {}: ", dbUser);

		final String dbPassword = parser.get("postgresPassword");
		log.info("postgresPassword {}: ", dbPassword);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			if (fromStep <= 0) {
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 0: LoadRecords\n**************************************");
				loadRecords(inputPath, dataPath);
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 0: DONE\n**************************************");
			}

			if (fromStep <= 1) {
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 1: Base Report\n**************************************");
				generateReport(spark, dataPath, outputPath);
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 1: DONE\n**************************************");
			}

			if (fromStep <= 2) {
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 2: OpenDOAR Report\n**************************************");
				generateOpenDoarReport(spark, outputPath, opendoarPath, loadOpenDoarStats(dbUrl, dbUser, dbPassword));
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 2: DONE\n**************************************");
			}

			if (fromStep <= 3) {
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 3: Type Vocabulary Report\n**************************************");
				generateVocTypeReport(spark, outputPath, typesReportPath);
				log
					.info(
						"\n**************************************\n* EXECUTING STEP 3: DONE\n**************************************");
			}
		});

	}

	private static void generateVocTypeReport(final SparkSession spark,
		final String reportPath,
		final String typesReportPath) {
		spark
			.read()
			.parquet(reportPath)
			.as(Encoders.bean(BaseRecordInfo.class))
			.flatMap(rec -> {
				final List<Tuple2<String, String>> list = new ArrayList<>();
				for (final String t1 : rec.getTypes()) {
					if (t1.startsWith("TYPE_NORM:")) {
						for (final String t2 : rec.getTypes()) {
							if (t2.startsWith("TYPE:")) {
								list
									.add(
										new Tuple2<>(StringUtils.substringAfter(t1, "TYPE_NORM:").trim(),
											StringUtils.substringAfter(t2, "TYPE:").trim()));
							}
						}
					}
				}
				return list.iterator();
			}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
			.distinct()
			.write()
			.mode(SaveMode.Overwrite)
			.format("parquet")
			.save(typesReportPath);

	}

	private static void generateOpenDoarReport(final SparkSession spark,
		final String reportPath,
		final String opendoarPath,
		final List<OpenDoarRepoStatus> repos) {

		final Dataset<OpenDoarRepoStatus> fromDB = spark.createDataset(repos, Encoders.bean(OpenDoarRepoStatus.class));

		final Dataset<OpenDoarRepoStatus> fromBASE = spark
			.read()
			.parquet(reportPath)
			.selectExpr("explode(collections) as collection")
			.where("isnotnull(collection.opendoarId) and character_length(collection.opendoarId)>0")
			.selectExpr("concat('opendoar____::',collection.opendoarId) as id")
			.groupBy(col("id"))
			.agg(count(col("id")))
			.map(row -> {
				final OpenDoarRepoStatus repo = new OpenDoarRepoStatus();
				repo.setId(row.getString(0));
				repo.getAggregations().put(BASE_DUMP, row.getLong(1));
				repo.setBaseCount(row.getLong(1));
				repo.setOpenaireCount(0);
				repo.setHighCompliance(false);
				return repo;
			}, Encoders.bean(OpenDoarRepoStatus.class));

		fromDB
			.joinWith(fromBASE, fromDB.col("id").equalTo(fromBASE.col("id")), "full_outer")
			.map(t -> merge(t._1, t._2), Encoders.bean(OpenDoarRepoStatus.class))
			.write()
			.mode(SaveMode.Overwrite)
			.format("parquet")
			.save(opendoarPath);
	}

	private static OpenDoarRepoStatus merge(final OpenDoarRepoStatus r1, final OpenDoarRepoStatus r2) {
		if (r1 == null) {
			return r2;
		}
		if (r2 == null) {
			return r1;
		}

		final OpenDoarRepoStatus r = new OpenDoarRepoStatus();
		r.setId(ObjectUtils.firstNonNull(r1.getId(), r2.getId()));
		r.setJurisdiction(ObjectUtils.firstNonNull(r1.getJurisdiction(), r2.getJurisdiction()));
		r.getAggregations().putAll(r1.getAggregations());
		r.getAggregations().putAll(r2.getAggregations());
		r.setHighCompliance(r1.isHighCompliance() || r2.isHighCompliance());
		r.setBaseCount(Math.max(r1.getBaseCount(), r2.getBaseCount()));
		r.setOpenaireCount(Math.max(r1.getOpenaireCount(), r2.getOpenaireCount()));

		return r;
	}

	private static List<OpenDoarRepoStatus> loadOpenDoarStats(final String dbUrl,
		final String dbUser,
		final String dbPassword) throws Exception {
		final List<OpenDoarRepoStatus> repos = new ArrayList<>();

		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {

			final String sql = IOUtils
				.toString(
					BaseAnalyzerJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/plugin/base/sql/opendoar-aggregation-status.sql"));

			dbClient.processResults(sql, row -> {
				try {
					final OpenDoarRepoStatus repo = new OpenDoarRepoStatus();
					repo.setId(row.getString("id"));
					repo.setJurisdiction(row.getString("jurisdiction"));
					repo.setBaseCount(0);
					repo.setHighCompliance(false);

					long sum = 0;
					for (final String s : (String[]) row.getArray("aggregations").getArray()) {
						final String api = StringUtils.substringBefore(s, "@@@");
						final long count = NumberUtils.toLong(StringUtils.substringAfter(s, "@@@"), 0);
						sum += count;
						repo.getAggregations().put(api, count);
						// This should recognize the HIGH Compliances: openaire*X.Y*
						if (s.contains("compliance: openaire")) {
							repo.setHighCompliance(true);
						}
					}
					repo.setOpenaireCount(sum);

					repos.add(repo);
					log.info("# FOUND OPENDOAR (DB): " + repo.getId());
				} catch (final SQLException e) {
					log.error("Error in SQL", e);
					throw new RuntimeException("Error in SQL", e);
				}
			});
		}
		return repos;
	}

	private static void loadRecords(final String inputPath, final String outputPath) throws Exception {
		try (final FileSystem fs = FileSystem.get(new Configuration());
			final AggregatorReport report = new AggregatorReport()) {

			final AtomicLong recordsCounter = new AtomicLong(0);

			final LongWritable key = new LongWritable();
			final Text value = new Text();

			try (final SequenceFile.Writer writer = SequenceFile
				.createWriter(
					fs.getConf(), SequenceFile.Writer.file(new Path(outputPath)), SequenceFile.Writer
						.keyClass(LongWritable.class),
					SequenceFile.Writer
						.valueClass(Text.class),
					SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DeflateCodec()))) {

				final BaseCollectorIterator iteraror = new BaseCollectorIterator(fs, new Path(inputPath), report);

				while (iteraror.hasNext()) {
					final String record = iteraror.next();

					final long i = recordsCounter.incrementAndGet();
					if ((i % 10000) == 0) {
						log.info("# Loaded records: " + i);
					}

					key.set(i);
					value.set(record);
					try {
						writer.append(key, value);
					} catch (final Throwable e1) {
						throw new RuntimeException(e1);
					}
				}

				log.info("# COMPLETED - Loaded records: " + recordsCounter.get());
			}
		}
	}

	private static void generateReport(final SparkSession spark,
		final String inputPath,
		final String targetPath) throws Exception {

		final JavaRDD<BaseRecordInfo> rdd = JavaSparkContext
			.fromSparkContext(spark.sparkContext())
			.sequenceFile(inputPath, LongWritable.class, Text.class)
			.map(s -> s._2.toString())
			.map(BaseAnalyzerJob::extractInfo);

		spark
			.createDataset(rdd.rdd(), Encoders.bean(BaseRecordInfo.class))
			.write()
			.mode(SaveMode.Overwrite)
			.format("parquet")
			.save(targetPath);
	}

	protected static BaseRecordInfo extractInfo(final String s) {
		try {
			final Document record = DocumentHelper.parseText(s);

			final BaseRecordInfo info = new BaseRecordInfo();

			final Set<String> paths = new LinkedHashSet<>();
			final Set<String> types = new LinkedHashSet<>();
			final List<BaseCollectionInfo> colls = new ArrayList<>();

			for (final Object o : record.selectNodes("//*|//@*")) {
				paths.add(((Node) o).getPath());

				if (o instanceof Element) {
					final Element n = (Element) o;

					final String nodeName = n.getName();

					if ("collection".equals(nodeName)) {
						final String collName = n.getText().trim();

						if (StringUtils.isNotBlank(collName)) {
							final BaseCollectionInfo coll = new BaseCollectionInfo();
							coll.setId(collName);
							coll.setOpendoarId(n.valueOf("@opendoar_id").trim());
							coll.setRorId(n.valueOf("@ror_id").trim());
							colls.add(coll);
						}
					} else if ("type".equals(nodeName)) {
						types.add("TYPE: " + n.getText().trim());
					} else if ("typenorm".equals(nodeName)) {
						types.add("TYPE_NORM: " + n.getText().trim());
					}
				}
			}

			info.setId(record.valueOf("//*[local-name() = 'header']/*[local-name() = 'identifier']").trim());
			info.getTypes().addAll(types);
			info.getPaths().addAll(paths);
			info.setCollections(colls);

			return info;
		} catch (final DocumentException e) {
			throw new RuntimeException(e);
		}
	}

}
