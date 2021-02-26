
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkCopyRelationsNoOpenorgs extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkUpdateEntity.class);

	public SparkCopyRelationsNoOpenorgs(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyRelationsNoOpenorgs.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkUpdateEntity(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	public void run(ISLookUpService isLookUpService) throws IOException {

		final String graphBasePath = parser.get("graphBasePath");
		final String workingPath = parser.get("workingPath");
		final String dedupGraphPath = parser.get("dedupGraphPath");

		log.info("graphBasePath:  '{}'", graphBasePath);
		log.info("workingPath:    '{}'", workingPath);
		log.info("dedupGraphPath: '{}'", dedupGraphPath);

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");
		final String outputPath = DedupUtility.createEntityPath(dedupGraphPath, "relation");

		removeOutputDir(spark, outputPath);

		JavaRDD<Relation> simRels = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(this::excludeOpenorgsRels);

		spark
			.createDataset(simRels.rdd(), Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

	private static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
	}

	private boolean excludeOpenorgsRels(Relation rel) {

		if (rel.getCollectedfrom() != null) {
			for (KeyValue k : rel.getCollectedfrom()) {
				if (k.getValue().equals("OpenOrgs Database")) {
					return false;
				}
			}
		}
		return true;
	}
}
