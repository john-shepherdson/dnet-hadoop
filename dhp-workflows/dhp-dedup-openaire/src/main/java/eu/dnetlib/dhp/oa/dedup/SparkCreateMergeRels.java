
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.google.common.hash.Hashing;
import com.kwartile.lib.cc.ConnectedComponent;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.SparkCompatUtils;
import scala.Tuple3;
import scala.collection.JavaConversions;

public class SparkCreateMergeRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateMergeRels.class);

	public SparkCreateMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
		parser.parseArgument(args);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookupUrl {}", isLookUpUrl);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"));
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkCreateMergeRels(parser, getSparkWithHiveSession(conf))
			.run(ISLookupClientFactory.getLookUpService(isLookUpUrl));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws ISLookUpException, DocumentException, IOException, SAXException {

		final String graphBasePath = parser.get("graphBasePath");
		final String workingPath = parser.get("workingPath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		int cut = Optional
			.ofNullable(parser.get("cutConnectedComponent"))
			.map(Integer::valueOf)
			.orElse(0);

		final String pivotHistoryDatabase = parser.get("pivotHistoryDatabase");

		log.info("connected component cut: '{}'", cut);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {
			final String subEntity = dedupConf.getWf().getSubEntityValue();
			final Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));

			log.info("Creating merge rels for: '{}'", subEntity);

			final int maxIterations = dedupConf.getWf().getMaxIterations();
			log.info("Max iterations {}", maxIterations);

			final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

			final Dataset<Row> simRels = spark
				.read()
				.load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity))
				.select("source", "target");

			UserDefinedFunction hashUDF = functions
				.udf(
					(String s) -> hash(s), DataTypes.LongType);

			// <hash(id), id>
			Dataset<Row> vertexIdMap = simRels
				.selectExpr("source as id")
				.union(simRels.selectExpr("target as id"))
				.distinct()
				.withColumn("vertexId", hashUDF.apply(functions.col("id")));

			// transform simrels into pairs of numeric ids
			final Dataset<Row> edges = spark
				.read()
				.load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity))
				.select("source", "target")
				.withColumn("source", hashUDF.apply(functions.col("source")))
				.withColumn("target", hashUDF.apply(functions.col("target")));

			// resolve connected components
			// ("vertexId", "groupId")
			Dataset<Row> cliques = ConnectedComponent
				.runOnPairs(edges, 50, spark);

			// transform "vertexId" back to its original string value
			// groupId is kept numeric as its string value is not used
			// ("id", "groupId")
			Dataset<Row> rawMergeRels = cliques
				.join(vertexIdMap, JavaConversions.asScalaBuffer(Collections.singletonList("vertexId")), "inner")
				.drop("vertexId")
				.distinct();

			// empty dataframe if historydatabase is not used
			Dataset<Row> pivotHistory = spark
				.createDataset(
					Collections.emptyList(),
					SparkCompatUtils.encoderFor(StructType.fromDDL("id STRING, lastUsage STRING")));

			if (StringUtils.isNotBlank(pivotHistoryDatabase)) {
				pivotHistory = spark
					.read()
					.table(pivotHistoryDatabase + "." + subEntity)
					.selectExpr("id", "lastUsage");
			}

			// depending on resulttype collectefrom and dateofacceptance are evaluated differently
			String collectedfromExpr = "false AS collectedfrom";
			String dateExpr = "'' AS date";

			if (Result.class.isAssignableFrom(clazz)) {
				if (Publication.class.isAssignableFrom(clazz)) {
					collectedfromExpr = "array_contains(collectedfrom.key, '" + ModelConstants.CROSSREF_ID
						+ "') AS collectedfrom";
				} else if (eu.dnetlib.dhp.schema.oaf.Dataset.class.isAssignableFrom(clazz)) {
					collectedfromExpr = "array_contains(collectedfrom.key, '" + ModelConstants.DATACITE_ID
						+ "') AS collectedfrom";
				}

				dateExpr = "dateofacceptance.value AS date";
			}

			// cap pidType at w3id as from there on they are considered equal
			UserDefinedFunction mapPid = udf(
				(String s) -> Math.min(PidType.tryValueOf(s).ordinal(), PidType.w3id.ordinal()), DataTypes.IntegerType);

			UserDefinedFunction validDate = udf((String date) -> {
				if (StringUtils.isNotBlank(date)
					&& date.matches(DatePicker.DATE_PATTERN) && DatePicker.inRange(date)) {
					return date;
				}
				return LocalDate.now().plusWeeks(1).toString();
			}, DataTypes.StringType);

			Dataset<Row> pivotingData = spark
				.read()
				.schema(Encoders.bean(clazz).schema())
				.json(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.selectExpr(
					"id",
					"regexp_extract(id, '^\\\\d+\\\\|([^_]+).*::', 1) AS pidType",
					collectedfromExpr,
					dateExpr)
				.withColumn("pidType", mapPid.apply(col("pidType"))) // ordinal of pid type
				.withColumn("date", validDate.apply(col("date")));

			// ordering to selected pivot id
			WindowSpec w = Window
				.partitionBy("groupId")
				.orderBy(
					col("lastUsage").desc_nulls_last(),
					col("pidType").asc_nulls_last(),
					col("collectedfrom").desc_nulls_last(),
					col("date").asc_nulls_last(),
					col("id").asc_nulls_last());

			Dataset<Relation> output = rawMergeRels
				.join(pivotHistory, JavaConversions.asScalaBuffer(Collections.singletonList("id")), "full")
				.join(pivotingData, JavaConversions.asScalaBuffer(Collections.singletonList("id")), "left")
				.withColumn("pivot", functions.first("id").over(w))
				.withColumn("position", functions.row_number().over(w))
				.flatMap(
					(FlatMapFunction<Row, Tuple3<String, String, String>>) (Row r) -> {
						String id = r.getAs("id");
						String dedupId = IdGenerator.generate(id);

						String pivot = r.getAs("pivot");
						String pivotDedupId = IdGenerator.generate(pivot);

						// filter out id == pivotDedupId
						// those are caused by claim expressed on pivotDedupId
						// information will be merged after creating deduprecord
						if (id.equals(pivotDedupId)) {
							return Collections.emptyIterator();
						}

						ArrayList<Tuple3<String, String, String>> res = new ArrayList<>();

						// singleton pivots have null groupId as they do not match rawMergeRels
						if (r.isNullAt(r.fieldIndex("groupId"))) {
							// the record is existing if it matches pivotingData
							if (!r.isNullAt(r.fieldIndex("collectedfrom"))) {
								// create relation with old dedup id
								res.add(new Tuple3<>(id, dedupId, null));
							}
							return res.iterator();
						}

						// this was a pivot in a previous graph but it has been merged into a new group with different
						// pivot
						if (!r.isNullAt(r.fieldIndex("lastUsage")) && !pivot.equals(id)
							&& !dedupId.equals(pivotDedupId)) {
							// materialize the previous dedup record as a merge relation with the new one
							res.add(new Tuple3<>(dedupId, pivotDedupId, null));
						}

						// add merge relations
						if (cut <= 0 || r.<Integer> getAs("position") <= cut) {
							res.add(new Tuple3<>(id, pivotDedupId, pivot));
						}

						return res.iterator();
					}, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
				.distinct()
				.flatMap(
					(FlatMapFunction<Tuple3<String, String, String>, Relation>) (Tuple3<String, String, String> r) -> {
						String id = r._1();
						String dedupId = r._2();
						String pivot = r._3();

						ArrayList<Relation> res = new ArrayList<>();
						res.add(rel(pivot, dedupId, id, ModelConstants.MERGES, dedupConf));
						res.add(rel(pivot, id, dedupId, ModelConstants.IS_MERGED_IN, dedupConf));

						return res.iterator();
					}, Encoders.bean(Relation.class));

			saveParquet(output, mergeRelPath, SaveMode.Overwrite);
		}
	}

	private static Relation rel(String pivot, String source, String target, String relClass, DedupConfig dedupConf) {

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

		// TODO calculate the trust value based on the similarity score of the elements in the CC

		r.setDataInfo(info);

		if (pivot != null) {
			KeyValue pivotKV = new KeyValue();
			pivotKV.setKey("pivot");
			pivotKV.setValue(pivot);

			r.setProperties(Arrays.asList(pivotKV));
		}
		return r;
	}

	public static long hash(final String id) {
		return Hashing.murmur3_128().hashString(id).asLong();
	}
}
