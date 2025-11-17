
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import eu.dnetlib.pace.tree.support.TreeProcessor;
import eu.dnetlib.pace.util.SparkCompatUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDeduper;

public class SparkRefineMergeRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkRefineMergeRels.class);
	private static final StructType rowSchema = new StructType(new StructField[] {
		new StructField("source", DataTypes.StringType, false, Metadata.empty()),
		new StructField("target", DataTypes.StringType, false, Metadata.empty())
	});

	private static double THRESHOLD = 0.7;

	public SparkRefineMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/refineMergeRels_parameters.json")));
		parser.parseArgument(args);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookupUrl {}", isLookUpUrl);

		SparkConf conf = new SparkConf();
//		conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"));
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkRefineMergeRels(parser, getSparkWithHiveSession(conf))
			.run(ISLookupClientFactory.getLookUpService(isLookUpUrl));
	}

	@Override
	void run(ISLookUpService isLookUpService) throws DocumentException, IOException, ISLookUpException, SAXException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		// for each dedup configuration
		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Processing mergerels for: '{}'", subEntity);

			final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

			SparkDeduper deduper = new SparkDeduper(dedupConf);

			// compute negative constraints
			Dataset<Row> entities = spark
				.read()
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.transform(deduper.model().parseJsonDataset());
			entities = appendNegativeConstraints(spark, entities, graphBasePath, subEntity);

			Dataset<Row> rawMergeRels = spark
				.read()
				.load(mergeRelPath)
				.as(Encoders.bean(Relation.class))
				.where("relClass == 'merges'")
				.select("source", "target")
				.join(entities, col("target").equalTo(entities.col("identifier")))
				.withColumnRenamed("source", "groupId");  // (groupId, target)

			Dataset<Row> conflictualIds = getConflictualIds(rawMergeRels);

			// collect conflictual merge relations
			Dataset<Row> conflictualMergeRels = rawMergeRels
					.join(conflictualIds, rawMergeRels.col("groupId").equalTo(col("conflictualGroupId")), "left_semi");

			Dataset<Row> cleanMergeRels = spark
					.read()
					.load(mergeRelPath)
					.as(Encoders.bean(Relation.class))
					.join(conflictualIds, expr("source = conflictualGroupId OR target = conflictualGroupId"), "left_anti");

			Dataset<Row> splitMergeRels = conflictualMergeRels
				.groupByKey((MapFunction<Row, String>) t -> t.getAs("groupId"), Encoders.STRING())
				.flatMapGroups(
					(FlatMapGroupsFunction<String, Row, Row>) (key, values) -> splitGroup(values, dedupConf),
						SparkCompatUtils.encoderFor(rowSchema));

			Dataset<Relation> output = splitMergeRels
					.flatMap(
						(FlatMapFunction<Row, Relation>) r -> {
							String dedupId = r.getString(0);
							String id = r.getString(1);

							ArrayList<Relation> res = new ArrayList<>();
							res.add(rel(dedupId, id, ModelConstants.MERGES, dedupConf));
							res.add(rel(id, dedupId, ModelConstants.IS_MERGED_IN, dedupConf));

							return res.iterator();
						}, Encoders.bean(Relation.class))
					.union(cleanMergeRels.as(Encoders.bean(Relation.class)));

			saveParquet(output, mergeRelPath + "_refined", SaveMode.Overwrite);
			renameParquet(spark, mergeRelPath + "_refined", mergeRelPath);
		}
	}

	// append a negativeConstraints column to the dataset to be used by the clique finder
	public static Dataset<Row> appendNegativeConstraints(SparkSession spark, Dataset<Row> entities,
		String graphBasePath, String subEntity) {

		switch (subEntity) {
			case "organization":
				Dataset<Row> entitiesWithNwo = entities
						.withColumn("negativeConstraints", when(lower(col("identifier")).contains("nwo"), array(lit("nwo"))).otherwise(array()));

				Dataset<Row> families = OpenorgsUtility
						.createFamilies(spark, graphBasePath + "/relation", ModelConstants.IS_PARENT_OF);

				return entitiesWithNwo
						.join(families, entitiesWithNwo.col("identifier").equalTo(families.col("id")), "left")
						.withColumn("negativeConstraints", array_union(col("negativeConstraints"), when(families.col("groupId").isNotNull(), array(families.col("groupId").cast("string"))).otherwise(array())))
						.drop("id", "groupId", "nwoConstraint", "familyConstraint");

			default:
				return spark.emptyDataFrame();
		}
	}

	// compute the list of conflictual ids (groups containing conflicts)
	private static Dataset<Row> getConflictualIds(Dataset<Row> rawMergeRels) {

		return rawMergeRels
			.select("groupId", "negativeConstraints")
			.groupBy("groupId")
			.agg(flatten(collect_list(col("negativeConstraints"))).alias("allConstraints"))
			.where(size(col("allConstraints"))
					.notEqual(size(array_distinct(col("allConstraints")))))
			.select(col("groupId").as("conflictualGroupId"));
	}

	private static Relation rel(String source, String target, String relClass, DedupConfig dedupConf) {

		String entityType = dedupConf.getWf().getEntityType();

		Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setRelClass(relClass);
		r.setRelType(entityType + entityType.substring(0, 1).toUpperCase() + entityType.substring(1));
		r.setSubRelType(ModelConstants.DEDUP);

		DataInfo info = new DataInfo();
		info.setDeletedbyinference(false);
		info.setInferred(true);
		info.setInvisible(false);
		info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
		Qualifier provenanceAction = new Qualifier();
		provenanceAction.setClassid(PROVENANCE_DEDUP);
		provenanceAction.setClassname(PROVENANCE_DEDUP);
		provenanceAction.setSchemeid(DNET_PROVENANCE_ACTIONS);
		provenanceAction.setSchemename(DNET_PROVENANCE_ACTIONS);
		info.setProvenanceaction(provenanceAction);

		r.setDataInfo(info);
		return r;
	}

	public static Iterator<Row> splitGroup(Iterator<Row> values, DedupConfig dedupConf) {

		List<Row> mergeRels = new ArrayList<>(); // to return the result
		List<Row> entities = StreamSupport
				.stream(Spliterators.spliteratorUnknownSize(values, 0), false)
				.collect(Collectors.toList());

		double[][] simMatrix = getSimMatrix(entities, dedupConf);

		List<Set<Integer>> cliques = allMaxCliquesGreedyFinder(simMatrix);

		for (Set<Integer> clique: cliques) {
			if (clique.size() > 1) {
				String newDedupId = IdGenerator.generate(entities.get(clique.stream().findFirst().orElseThrow(RuntimeException::new)).getAs("identifier"));

				for (Integer i : clique) {
					Row r = entities.get(i);
					mergeRels.add(RowFactory.create(newDedupId, r.getAs("identifier")));
				}
			}
		}

		return mergeRels.iterator();
	}

	public static List<Set<Integer>> allMaxCliquesGreedyFinder(double[][] simMatrix) {

		int n = simMatrix.length;
		boolean[] used = new boolean[n];
		List<Set<Integer>> cliques = new ArrayList<>();

		for (int i = 0; i < n; i++) {
			if (used[i]) continue;

			Set<Integer> clique = new HashSet<>();
			clique.add(i);

			for (int j = 0; j < n; j++) {
				if (i == j || used[j]) continue;

				boolean valid = true;
				double totalSim = 0.0;
				int count = 0;

				for (int member : clique) {
					double sim = simMatrix[j][member];
					if (sim == -1 || simMatrix[member][j] == -1) {
						valid = false;
						break;
					}
					totalSim += sim;
					count++;
				}

				if (valid && count > 0 && (totalSim / count) >= THRESHOLD) {
					clique.add(j);
				}
			}

			for (int node : clique) {
				used[node] = true;
			}

			cliques.add(clique);
		}

		return cliques;
	}

	public static double[][] getSimMatrix(List<Row> entities, DedupConfig dedupConf) {

		TreeProcessor treeProcessor = new TreeProcessor(dedupConf);

		int n = entities.size();
		double[][] simMatrix = new double[n][n];

		for (int i = 0; i < n; i++) {
			Row a = entities.get(i);

			for (int j = i; j < n; j++) {
				Row b = entities.get(j);

				if( j == i) {
					simMatrix[i][j] = 1.0; // similarity with itself
				} else {
					double sim = weigh(a, b, treeProcessor);
					simMatrix[i][j] = sim;
					simMatrix[j][i] = sim;
				}
			}
		}

		return simMatrix;
	}

	public static double weigh(Row a, Row b, TreeProcessor treeProcessor) {
		List<String> negativeConstraintsA = Arrays.asList(a.schema().fieldNames()).contains("negativeConstraints")
				? a.getList(a.fieldIndex("negativeConstraints"))
				: Collections.emptyList();
		List<String> negativeConstraintsB = Arrays.asList(b.schema().fieldNames()).contains("negativeConstraints")
				? b.getList(b.fieldIndex("negativeConstraints"))
				: Collections.emptyList();

		if (!negativeConstraintsA.isEmpty() && !negativeConstraintsB.isEmpty()
				&& !Collections.disjoint(negativeConstraintsA, negativeConstraintsB)) {
			return -1;
		}

		return treeProcessor.computeScore(a, b);

	}

}
