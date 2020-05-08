
package eu.dnetlib.dedup;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.pace.config.DedupConfig;

public class SparkCreateDedupRecord {
	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateDedupRecord.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/sx/dedup/dedupRecord_parameters.json")));
		parser.parseArgument(args);
		final SparkSession spark = SparkSession
			.builder()
			.appName(SparkCreateDedupRecord.class.getSimpleName())
			.master(parser.get("master"))
			.getOrCreate();

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		final String sourcePath = parser.get("sourcePath");
		final String entity = parser.get("entity");
		final String dedupPath = parser.get("dedupPath");
		final DedupConfig dedupConf = DedupConfig.load(parser.get("dedupConf"));

		final JavaRDD<OafEntity> dedupRecord = DedupRecordFactory
			.createDedupRecord(
				sc,
				spark,
				DedupUtility.createMergeRelPath(dedupPath, entity),
				DedupUtility.createEntityPath(sourcePath, entity),
				OafEntityType.valueOf(entity),
				dedupConf);
		dedupRecord
			.map(
				r -> {
					ObjectMapper mapper = new ObjectMapper();
					return mapper.writeValueAsString(r);
				})
			.saveAsTextFile(dedupPath + "/" + entity + "/dedup_records");
	}
}
