
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class SparkCopyRelationsNoOpenorgs extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCopyRelationsNoOpenorgs.class);

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

		new SparkCopyRelationsNoOpenorgs(parser, getSparkSession(conf))
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

		JavaRDD<Relation> simRels = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(x -> !isOpenorgsDedupRel(x));

		if (log.isDebugEnabled()) {
			log.debug("Number of non-Openorgs relations collected: {}", simRels.count());
		}

		save(spark.createDataset(simRels.rdd(), Encoders.bean(Relation.class)), outputPath, SaveMode.Overwrite);
	}

}
