
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import scala.collection.JavaConverters;

public class SparkCreateDedupRecord extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateDedupRecord.class);

	public static final String ROOT_TRUST = "0.8";

	public SparkCreateDedupRecord(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkCreateDedupRecord(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws ISLookUpException, DocumentException, IOException, SAXException {

		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {
			String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Creating deduprecords for: '{}'", subEntity);

			final String outputPath = DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity);
			removeOutputDir(spark, outputPath);

			final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);
			final String entityPath = DedupUtility.createEntityPath(graphBasePath, subEntity);

			final Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));
			final DataInfo dataInfo = getDataInfo(dedupConf);
			DedupRecordFactory
				.createDedupRecord(spark, dataInfo, mergeRelPath, entityPath, clazz)
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(outputPath);

			log.info("Updating mergerels for: '{}'", subEntity);
			final Dataset<Row> dedupIds = spark
				.read()
				.schema("`id` STRING, `mergedIds` ARRAY<STRING>")
				.json(outputPath)
				.selectExpr("id as source", "explode(mergedIds) as target");
			spark
				.read()
				.load(mergeRelPath)
				.where("relClass == 'merges'")
				.join(dedupIds, JavaConverters.asScalaBuffer(Arrays.asList("source", "target")).toSeq(), "left_semi")
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.save(workingPath + "/mergerel_filtered");

			final Dataset<Row> validRels = spark.read().load(workingPath + "/mergerel_filtered");

			final Dataset<Row> filteredMergeRels = validRels
				.union(
					validRels
						.withColumnRenamed("source", "source_tmp")
						.withColumnRenamed("target", "target_tmp")
						.withColumn("relClass", functions.lit(ModelConstants.IS_MERGED_IN))
						.withColumnRenamed("target_tmp", "source")
						.withColumnRenamed("source_tmp", "target"));

			saveParquet(filteredMergeRels, mergeRelPath, SaveMode.Overwrite);
			removeOutputDir(spark, workingPath + "/mergerel_filtered");
		}
	}

	private static DataInfo getDataInfo(DedupConfig dedupConf) {
		DataInfo info = new DataInfo();
		info.setDeletedbyinference(false);
		info.setInferred(true);
		info.setInvisible(false);
		info.setTrust(ROOT_TRUST);
		info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
		Qualifier provenance = new Qualifier();
		provenance.setClassid(PROVENANCE_DEDUP);
		provenance.setClassname(PROVENANCE_DEDUP);
		provenance.setSchemeid(DNET_PROVENANCE_ACTIONS);
		provenance.setSchemename(DNET_PROVENANCE_ACTIONS);
		info.setProvenanceaction(provenance);
		return info;
	}
}
