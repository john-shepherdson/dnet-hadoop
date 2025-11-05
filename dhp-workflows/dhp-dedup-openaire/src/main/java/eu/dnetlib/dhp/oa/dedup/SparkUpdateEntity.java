
package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SparkUpdateEntity extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkUpdateEntity.class);

	public SparkUpdateEntity(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateEntity.class
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
		final String actionSetId = parser.get("actionSetId");
		final String graphBasePath = parser.get("graphBasePath");
		final String workingPath = parser.get("workingPath");
		final String dedupGraphPath = parser.get("dedupGraphPath");

		log.info("actionSetId:  '{}'", actionSetId);
		log.info("graphBasePath:  '{}'", graphBasePath);
		log.info("workingPath:    '{}'", workingPath);
		log.info("dedupGraphPath: '{}'", dedupGraphPath);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		for (Map.Entry<EntityType, Class> e : ModelSupport.entityTypes.entrySet()) {
			final EntityType type = e.getKey();
			final Class clazz = e.getValue();
			final String outputPath = dedupGraphPath + "/" + type;
			removeOutputDir(spark, outputPath);
			final String ip = DedupUtility.createEntityPath(graphBasePath, type.toString());
			if (HdfsSupport.exists(ip, sc.hadoopConfiguration())) {
				Dataset<String> sourceEntity = spark.read().text(DedupUtility.createEntityPath(graphBasePath, type.toString())).as(Encoders.STRING());

				if (mergeRelExists(sc, workingPath, type.toString())) {

					final String mergeRelPath = DedupUtility
						.createMergeRelPath(workingPath, actionSetId, type.toString());
					final String dedupRecordPath = DedupUtility
						.createDedupRecordPath(workingPath, actionSetId, type.toString());

					final Dataset<Relation> rel = spark
						.read()
						.load(mergeRelPath)
						.as(Encoders.bean(Relation.class));

					final Dataset<Row> mergedIds = rel
						.where("relClass == 'merges'")
						.where("source != target")
						.select(rel.col("target"))
						.distinct()
						.selectExpr("target as id", "TRUE as merged");


					Dataset<Row> entitiesWithId = sourceEntity
							.selectExpr("get_json_object(value, '$.id') as id", "value");

					if (type == EntityType.organization) {// exclude root records from organizations
						Dataset<Row> roots = rel
								.where("relClass == 'merges'")
								.selectExpr("source as id")
								.distinct();

						entitiesWithId =  entitiesWithId.join(roots, DHPUtils.toSeq(Collections.singletonList("id")).toSeq(), "left_anti");
					}

					Dataset<String> map = entitiesWithId
						.join(mergedIds, DHPUtils.toSeq(Collections.singletonList("id")).toSeq(), "left")
						.map((MapFunction<Row, String>)  row -> {
							if (!row.isNullAt(row.fieldIndex("merged")) && row.<Boolean> getAs("merged")) {
								return updateDeletedByInference(row.getAs("value"), clazz);
							}
							return row.getAs("value");
						}, Encoders.STRING());

					sourceEntity = map.union(spark.read().text(dedupRecordPath).as(Encoders.STRING()));
				}

				saveText(sourceEntity, outputPath, SaveMode.Overwrite);
			}
		}
	}

	public boolean mergeRelExists(JavaSparkContext sc, String basePath, String entity) throws IOException {

		boolean result = false;

		Path p =  new Path(basePath);

		FileSystem fileSystem = p.getFileSystem(sc.hadoopConfiguration());
		FileStatus[] fileStatuses = fileSystem.listStatus(p);

		for (FileStatus fs : fileStatuses) {
			final Path mergeRelPath = new Path(
				DedupUtility.createMergeRelPath(basePath, fs.getPath().getName(), entity));
			if (fs.isDirectory() && fileSystem.exists(mergeRelPath)) {
				result = true;
			}
		}

		return result;
	}

	private static <T extends OafEntity> String updateDeletedByInference(
		final String json, final Class<T> clazz) {
		try {
			Oaf entity = OBJECT_MAPPER.readValue(json, clazz);
			if (entity.getDataInfo() == null)
				entity.setDataInfo(new DataInfo());
			entity.getDataInfo().setDeletedbyinference(true);
			return OBJECT_MAPPER.writeValueAsString(entity);
		} catch (IOException e) {
			throw new RuntimeException("Unable to convert json", e);
		}
	}
}
