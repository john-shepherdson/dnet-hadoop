
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 * Creates new Relations (as in eu.dnetlib.dhp.schema.dump.oaf.graph.Relation) from the information in the Entity. The
 * new Relations are created for the datasource in the collectedfrom and hostedby elements and for the context related
 * to communities and research initiative/infrastructures. For collectedfrom elements it creates: datasource -> provides
 * -> result and result -> isProvidedBy -> datasource For hostedby elements it creates: datasource -> hosts -> result
 * and result -> isHostedBy -> datasource For context elements it creates: context <-> isRelatedTo <-> result. Note for
 * context: it gets the first provenance in the dataInfo. If more than one is present the others are not dumped
 */
public class Extractor implements Serializable {

	public void run(Boolean isSparkSessionManaged,
		String inputPath,
		String outputPath,
		Class<? extends Result> inputClazz,
		String communityMapPath) {

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				extractRelationResult(
					spark, inputPath, outputPath, inputClazz, Utils.getCommunityMap(spark, communityMapPath));
			});
	}

	private <R extends Result> void extractRelationResult(SparkSession spark,
		String inputPath,
		String outputPath,
		Class<R> inputClazz,
		CommunityMap communityMap) {

		Set<Integer> hashCodes = new HashSet<>();

		Utils
			.readPath(spark, inputPath, inputClazz)
			.flatMap((FlatMapFunction<R, Relation>) value -> {
				List<Relation> relationList = new ArrayList<>();
				Optional
					.ofNullable(value.getInstance())
					.ifPresent(inst -> inst.forEach(instance -> {
						Optional
							.ofNullable(instance.getCollectedfrom())
							.ifPresent(
								cf -> getRelatioPair(
									value, relationList, cf,
									ModelConstants.IS_PROVIDED_BY, ModelConstants.PROVIDES, hashCodes));
						Optional
							.ofNullable(instance.getHostedby())
							.ifPresent(
								hb -> getRelatioPair(
									value, relationList, hb,
									Constants.IS_HOSTED_BY, Constants.HOSTS, hashCodes));
					}));
				Set<String> communities = communityMap.keySet();
				Optional
					.ofNullable(value.getContext())
					.ifPresent(contexts -> contexts.forEach(context -> {
						String id = context.getId();
						if (id.contains(":")) {
							id = id.substring(0, id.indexOf(":"));
						}
						if (communities.contains(id)) {
							String contextId = Utils.getContextId(id);
							Provenance provenance = Optional
								.ofNullable(context.getDataInfo())
								.map(
									dinfo -> Optional
										.ofNullable(dinfo.get(0).getProvenanceaction())
										.map(
											paction -> Provenance
												.newInstance(
													paction.getClassid(),
													dinfo.get(0).getTrust()))
										.orElse(null))
								.orElse(null);
							Relation r = getRelation(
								value.getId(), contextId,
								Constants.RESULT_ENTITY,
								Constants.CONTEXT_ENTITY,
								ModelConstants.RELATIONSHIP, ModelConstants.IS_RELATED_TO, provenance);
							if (!hashCodes.contains(r.hashCode())) {
								relationList
									.add(r);
								hashCodes.add(r.hashCode());
							}
							r = getRelation(
								contextId, value.getId(),
								Constants.CONTEXT_ENTITY,
								Constants.RESULT_ENTITY,
								ModelConstants.RELATIONSHIP,
								ModelConstants.IS_RELATED_TO, provenance);
							if (!hashCodes.contains(r.hashCode())) {
								relationList
									.add(
										r);
								hashCodes.add(r.hashCode());
							}

						}

					}));

				return relationList.iterator();
			}, Encoders.bean(Relation.class))
				.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

	private static <R extends Result> void getRelatioPair(R value, List<Relation> relationList, KeyValue cf,
		String result_dtasource, String datasource_result,
		Set<Integer> hashCodes) {
		Provenance provenance = Optional
			.ofNullable(cf.getDataInfo())
			.map(
				dinfo -> Optional
					.ofNullable(dinfo.getProvenanceaction())
					.map(
						paction -> Provenance
							.newInstance(
								paction.getClassname(),
								dinfo.getTrust()))
					.orElse(
						Provenance
							.newInstance(
								eu.dnetlib.dhp.oa.graph.dump.Constants.HARVESTED,
								eu.dnetlib.dhp.oa.graph.dump.Constants.DEFAULT_TRUST)))
			.orElse(
				Provenance
					.newInstance(
						eu.dnetlib.dhp.oa.graph.dump.Constants.HARVESTED,
						eu.dnetlib.dhp.oa.graph.dump.Constants.DEFAULT_TRUST));
		Relation r = getRelation(
			value.getId(),
			cf.getKey(), Constants.RESULT_ENTITY, Constants.DATASOURCE_ENTITY,
			result_dtasource, ModelConstants.PROVISION,
			provenance);
		if (!hashCodes.contains(r.hashCode())) {
			relationList
				.add(r);
			hashCodes.add(r.hashCode());
		}

		r = getRelation(
			cf.getKey(), value.getId(),
			Constants.DATASOURCE_ENTITY, Constants.RESULT_ENTITY,
			datasource_result, ModelConstants.PROVISION,
			provenance);

		if (!hashCodes.contains(r.hashCode())) {
			relationList
				.add(r);
			hashCodes.add(r.hashCode());
		}

	}

	private static Relation getRelation(String source, String target, String sourceType, String targetType,
		String relName, String relType, Provenance provenance) {
		Relation r = new Relation();
		r.setSource(Node.newInstance(source, sourceType));
		r.setTarget(Node.newInstance(target, targetType));
		r.setReltype(RelType.newInstance(relName, relType));
		r.setProvenance(provenance);
		return r;
	}
}
